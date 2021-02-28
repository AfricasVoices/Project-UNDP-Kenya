import argparse
import csv
import json
import sys

from core_data_modules.cleaners import Codes
from core_data_modules.logging import Logger
from core_data_modules.traced_data.io import TracedDataJsonIO
from id_infrastructure.firestore_uuid_table import FirestoreUuidTable
from storage.google_cloud import google_cloud_utils

from src.lib import PipelineConfiguration
from configuration.code_schemes import CodeSchemes

log = Logger(__name__)

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Exports phone numbers of people who opted into AVF's Kenya "
                                                 "Pool at the end of the project")

    parser.add_argument("target_counties", nargs="*", metavar="target-counties",
                        help="List of kenyan counties to derive contacts from, use the StringValue in kenya_county code scheme"
                             "or don't pass the argument to generate contacts from all counties")
    parser.add_argument("google_cloud_credentials_file_path", metavar="google-cloud-credentials-file-path",
                        help="Path to a Google Cloud service account credentials file to use to access the "
                             "credentials bucket")
    parser.add_argument("pipeline_configuration_file_path", metavar="pipeline-configuration-file",
                        help="Path to the pipeline configuration json file")
    parser.add_argument("traced_data_paths", metavar="traced-data-paths", nargs="+",
                        help="Paths to the traced data files (either messages or individuals) to extract phone "
                             "numbers from")
    parser.add_argument("opt_in_dataset_name", metavar="opt-in-dataset-name",
                        help="`dataset_name` of the coding plan to use to search for opt-ins")
    parser.add_argument("csv_output_file_path", metavar="csv-output-file-path",
                        help="Path to a CSV file to write the contacts to. "
                             "Exported file is in a format suitable for direct upload to Rapid Pro")

    args = parser.parse_args()

    target_counties = args.target_counties
    google_cloud_credentials_file_path = args.google_cloud_credentials_file_path
    pipeline_configuration_file_path = args.pipeline_configuration_file_path
    traced_data_paths = args.traced_data_paths
    opt_in_dataset_name = args.opt_in_dataset_name
    csv_output_file_path = args.csv_output_file_path

    sys.setrecursionlimit(10000)

    log.info("Loading Pipeline Configuration File...")
    with open(pipeline_configuration_file_path) as f:
        pipeline_configuration = PipelineConfiguration.from_configuration_file(f)
    Logger.set_project_name(pipeline_configuration.pipeline_name)
    log.debug(f"Pipeline name is {pipeline_configuration.pipeline_name}")

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

    uuids = set()
    skipped = 0
    county_counts = {county:0 for county in target_counties}
    for path in traced_data_paths:
        # Load the traced data
        log.info(f"Loading previous traced data from file '{path}'...")
        with open(path) as f:
            data = TracedDataJsonIO.import_jsonl_to_traced_data_iterable(f)
        log.info(f"Loaded {len(data)} traced data objects")

        # Filter for participants that opted-in via the close-out.
        for td in data:
            if td["consent_withdrawn"] == Codes.TRUE:
                continue

            for plan in PipelineConfiguration.RQA_CODING_PLANS:
                if plan.dataset_name != opt_in_dataset_name:
                    continue

                opt_in = False
                for cc in plan.coding_configurations:
                    for label in td[cc.coded_field]:
                        code = cc.code_scheme.get_code_with_code_id(label["CodeID"])
                        if code.meta_code == "opt-in":
                            opt_in = True

                if opt_in:
                    if len(target_counties) != 0:
                        if td["county_coded"] == Codes.STOP:
                            continue

                        county_name = CodeSchemes.KENYA_COUNTY.get_code_with_code_id(td["county_coded"]["CodeID"]).string_value
                        #TODO: validate if county_name in target_counties is in KENYA_COUNTY StringValue
                        if county_name in target_counties:
                            uuids.add(td["uid"])
                            county_counts[county_name] += 1
                    else:
                        uuids.add(td["uid"])
                else:
                    skipped += 1

    log.info(f"Found (per-location counts: {county_counts}) a total of {len(uuids)} contacts (skipped {skipped} items)")

    # Convert the uuids to phone numbers
    log.info(f"Converting {len(uuids)} uuids to phone numbers...")
    uuid_phone_number_lut = phone_number_uuid_table.uuid_to_data_batch(uuids)
    phone_numbers = set()
    for uuid in uuids:
        phone_numbers.add(f"+{uuid_phone_number_lut[uuid]}")
    log.info(f"Successfully converted {len(phone_numbers)} uuids to phone numbers.")

    # Export contacts CSV
    log.warning(f"Exporting {len(phone_numbers)} phone numbers to {csv_output_file_path}...")
    with open(csv_output_file_path, "w") as f:
        writer = csv.DictWriter(f, fieldnames=["URN:Tel", "Name"], lineterminator="\n")
        writer.writeheader()

        for n in phone_numbers:
            writer.writerow({
                "URN:Tel": n
            })
        log.info(f"Wrote {len(phone_numbers)} contacts to {csv_output_file_path}")
