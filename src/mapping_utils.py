import matplotlib.pyplot as plt
import numpy as np
import pandas
from core_data_modules.logging import Logger
from mapclassify import FisherJenks
from matplotlib.colors import LinearSegmentedColormap
from matplotlib.patches import Patch

log = Logger(__name__)


class MappingUtils(object):
    AVF_COLOR_MAP = LinearSegmentedColormap.from_list("avf_color_map", ["#ffffff", "#993e46"])
    WATER_COLOR = "#edf5ff"

    @classmethod
    def plot_frequency_map(cls, geo_data, admin_id_column, frequencies, label_position_columns=None,
                           callout_position_columns=None, ax=None):
        """
        Plots a map of the given geo data with a choropleth showing the frequency of responses in each administrative
        region.

        The map is plotted to the specified axes or to the active matplotlib figure.
        Use matplotlib.pyplot to access and manipulate the result.

        :param geo_data: GeoData to plot.
        :type geo_data: geopandas.GeoDataFrame
        :param admin_id_column: Column in `geo_data` of the administrative region ids.
        :type admin_id_column: str
        :param frequencies: Dictionary of admin_id -> frequency.
        :type frequencies: dict of str -> int
        :param label_position_columns: A tuple specifying which columns in the `geo_data` contain the positions to draw
                                       each frequency label at, or None.
                                       The format is (X Position Column, Y Position Column). Positions should be in
                                       the same coordinate system as the geometry, and represent the vertical and
                                       horizontal center position of the drawn label.
                                       If None, no frequency labels are drawn.
        :type label_position_columns: (str, str) | None
        :param callout_position_columns: A tuple specifying which columns in the `geo_data` contain the positions to
                                         draw callout lines to, or None.
                                         The format is (X Position Column, Y Position Column). Positions should be in
                                         the same coordinate system as the geometry, and represent the target location
                                         to draw the callout line to. The callout line is drawn from the label_position
                                         for this feature.
                                         If None, no callout lines are drawn.
        :type callout_position_columns: (str, str) | None
        :param ax: Axes on which to draw the plot. If None, draws to a new figure.
        :type ax: matplotlib.pyplot.Artist | None
        """
        # Class the frequencies using the Fisher-Jenks method, a standard GIS algorithm for choropleth classification.
        # Using this method prevents a region with a vastly higher frequency than the others (e.g. a capital city)
        # from using up all of the colour range, as would happen with a linear scale.
        # Ignores zeros when classing, so that 0s are not included in the same class as other lower numbers, then adds
        # the 0 back in when converting from classes to bin edges.
        frequencies_to_class = [f for f in frequencies.values() if f != 0]
        number_of_classes = min(5, len(set(frequencies_to_class)))
        bin_edges = [0]
        if number_of_classes > 0:
            bin_edges.extend(FisherJenks(np.array(frequencies_to_class), k=number_of_classes).bins)

        # Get the color for each region by searching for the appropriate bin for each frequency.
        colors = []
        for i, admin_region in geo_data.iterrows():
            frequency = frequencies[admin_region[admin_id_column]]
            bin_id = [i for i, b in enumerate(bin_edges) if b >= frequency][0]  # Index of first bin edge >= frequency
            colors.append(cls.AVF_COLOR_MAP(0 if bin_id == 0 else float(bin_id) / number_of_classes))

        # Plot the choropleth map.
        ax = geo_data.plot(ax=ax, color=colors, linewidth=0.1, edgecolor="black")
        plt.axis("off")

        # Add the choropleth legend.
        legend_elements = [
            Patch(label="0", facecolor=cls.AVF_COLOR_MAP(0), linewidth=0.1, edgecolor="black")
        ]
        for bin_id in range(1, len(bin_edges)):
            range_min = bin_edges[bin_id - 1] + 1
            range_max = bin_edges[bin_id]
            legend_elements.append(Patch(
                label=range_min if range_min == range_max else f"{range_min} - {range_max}",
                facecolor=cls.AVF_COLOR_MAP(float(bin_id) / number_of_classes),
                linewidth=0.1, edgecolor="black"
            ))
        ax.legend(handles=legend_elements, title="Participants", title_fontsize=6, loc="lower right",
                  frameon=False, handlelength=1.8, handleheight=1.8, labelspacing=0, prop=dict(size=5.5))

        # Add a label to each administrative region showing its absolute frequency.
        # The font size and label position names are currently hard-coded for Somali Regions counties.
        # TODO: Modify once per-map configuration needs are better understood by testing on other maps.
        if label_position_columns is not None:
            for i, admin_region in geo_data.iterrows():
                # Skip rows that don't have a label x/y position set.
                # If we don't skip here, we'll get a very cryptic StopIteration error much later.
                if pandas.isna(admin_region[label_position_columns[0]]) or \
                        pandas.isna(admin_region[label_position_columns[1]]):
                    log.warning(f"No label positions provided for admin region '{admin_region[admin_id_column]}'")
                    continue

                # Set label and callout positions from the features in the geo_data,
                # translating from the geo_data format to the matplotlib format.
                if callout_position_columns is None or pandas.isna(admin_region[callout_position_columns[0]]):
                    # Draw label only.
                    xy = (admin_region[label_position_columns[0]], admin_region[label_position_columns[1]])
                    xytext = None
                else:
                    # Draw label and callout line.
                    xy = (admin_region[callout_position_columns[0]], admin_region[callout_position_columns[1]])
                    xytext = (admin_region[label_position_columns[0]], admin_region[label_position_columns[1]])

                plt.annotate(text=frequencies[admin_region[admin_id_column]],
                             xy=xy, xytext=xytext,
                             arrowprops=dict(facecolor="black", arrowstyle="-", linewidth=0.1, shrinkA=0, shrinkB=0),
                             ha="center", va="center", fontsize=3.8)
