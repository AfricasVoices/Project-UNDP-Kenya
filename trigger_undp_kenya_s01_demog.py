import argparse
import json

from core_data_modules.logging import Logger
from dateutil.parser import isoparse
from id_infrastructure.firestore_uuid_table import FirestoreUuidTable
from rapid_pro_tools.rapid_pro_client import RapidProClient
from storage.google_cloud import google_cloud_utils

from src.lib import PipelineConfiguration

log = Logger(__name__)

# TODO: Read these from pipeline configuration rather than hard-coding
rapid_pro_domain = "textit.in"
rapid_pro_token_url = "gs://avf-credentials/covid19-2-text-it-token.txt"
demog_flow_name = "undp_kenya_s01_demog"
demogs_attempted_variable = "undp_kenya_s01_demogs_attempted"

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Triggers demogs to people who haven't yet received them")

    parser.add_argument("google_cloud_credentials_file_path", metavar="google-cloud-credentials-file-path",
                        help="Path to a Google Cloud service account credentials file to use to access the "
                             "credentials bucket")
    parser.add_argument("pipeline_configuration_file_path", metavar="pipeline-configuration-file",
                        help="Path to the pipeline configuration json file")
    parser.add_argument("avf_uuid_file_path", metavar="avf-uuid-file-path",
                        help="Path to a json file containing a list of avf uuids that it's safe to trigger "
                             "the demog flow to")

    args = parser.parse_args()

    google_cloud_credentials_file_path = args.google_cloud_credentials_file_path
    pipeline_configuration_file_path = args.pipeline_configuration_file_path
    avf_uuid_file_path = args.avf_uuid_file_path

    log.info("Loading Pipeline Configuration File...")
    with open(pipeline_configuration_file_path) as f:
        pipeline_configuration = PipelineConfiguration.from_configuration_file(f)
    Logger.set_project_name(pipeline_configuration.pipeline_name)
    log.debug(f"Pipeline name is {pipeline_configuration.pipeline_name}")

    log.info("Downloading Rapid Pro access token...")
    rapid_pro_token = google_cloud_utils.download_blob_to_string(
        google_cloud_credentials_file_path, rapid_pro_token_url).strip()

    rapid_pro = RapidProClient(rapid_pro_domain, rapid_pro_token)

    log.info("Downloading Firestore UUID Table credentials...")
    firestore_uuid_table_credentials = json.loads(google_cloud_utils.download_blob_to_string(
        google_cloud_credentials_file_path,
        pipeline_configuration.phone_number_uuid_table.firebase_credentials_file_url
    ))

    phone_number_uuid_table = FirestoreUuidTable(
        pipeline_configuration.phone_number_uuid_table.table_name,
        firestore_uuid_table_credentials,
        "avf-phone-uuid-"
    )
    log.info("Initialised the Firestore UUID table")

    log.info(f"Loading the uuids that are safe to send to")
    with open(avf_uuid_file_path) as f:
        safe_uuids = json.load(f)
    log.info(f"Loaded {len(safe_uuids)} uuids")

    log.info(f"Re-identifying the uuids")
    safe_numbers = phone_number_uuid_table.uuid_to_data_batch(safe_uuids).values()
    safe_urns = {f"tel:+{number}" for number in safe_numbers}
    log.info(f"Re-identified {len(safe_urns)} uuids")

    log.info("Downloading the latest contacts fields from Rapid Pro")
    contacts = rapid_pro.get_raw_contacts()
    urn_to_contact = dict()
    for c in contacts:
        for urn in c.urns:
            urn_to_contact[urn] = c

    log.info(f"Filtering the urns who haven't received demogs before")
    urns_to_send_to = {
        urn for urn in safe_urns
        if urn_to_contact[urn].fields[demogs_attempted_variable] is None
        # Filter for people created since the project started. People created before then went through
        # the demogs flow, but may not have been asked any questions/been assigned the demogs_attempted
        # variable if they had already completed them last season, so we can skip these.
        and urn_to_contact[urn].created_on > pipeline_configuration.project_start_date
        # Filter out people who received the ke_urban demogs on the morning of Nov. 19th.
        # These dates should exactly match those in the key remapping in the pipeline config
        and not isoparse("2020-11-18T23:20:00+03:00") < urn_to_contact[urn].created_on < isoparse("2020-11-19T13:40:00+03:00")
        and not isoparse("2020-11-18T09:23:00+03:00") < urn_to_contact[urn].created_on < isoparse("2020-11-18T18:13:00+03:00")
    }
    log.info(f"Filtered for {len(urns_to_send_to)}/{len(safe_urns)} urns")

    log.info(f"Triggering the demog flow for {len(urns_to_send_to)} safe URNs who haven't received demog questions yet")
    flow_id = rapid_pro.get_flow_id(demog_flow_name)
    for i, urn in enumerate(urns_to_send_to):
        log.info(f"Triggering {i + 1}/{len(urns_to_send_to)}...")
        log.debug(urn)
        # (The API supports sending up to 100 URNs at once. This code opts to send one at a time, which is slower but
        #  keeps the code simple and makes it easier to debug by preventing partial successes/failures)
        # TODO: Move to RapidProClient. For now, use the underlying TembaClient object to create a flow start.
        rapid_pro.rapid_pro.create_flow_start(flow_id, urns=[urn], restart_participants=False)
