import datetime
import os.path
import sys
from pathlib import Path

import pymupdf

sys.path.append(os.getcwd())
from src.base.log_config import get_logger
from benchmarking.test_runner.plotting.boxes import (
    MainTitleBox,
    SectionTitleBox,
    SectionContentBox,
    SectionSubtitleBox,
)

logger = get_logger()

BASE_DIR = Path(__file__).resolve().parent.parent.parent  # heiDGAF directory


class PDFOverviewGenerator:
    """Combines multiple plots and test information in a PDF document."""

    def __init__(self):
        self.page_width, self.page_height = 595, 842  # page dimension: A4 portrait
        self.standard_page_margin = {"left": 50, "right": 50, "top": 50, "bottom": 80}

        self.document = pymupdf.open()
        self.boxes = {}
        self.row_heights = {}

    def setup_first_page_layout(self):
        """Adds the first page and configures its layout."""
        page_margin, usable_width, usable_height = self.__prepare_overview_page()

        self.boxes["overview_page"]["main_title_row"].append(
            MainTitleBox(
                page=self.document[0],  # first page
                page_margin=page_margin,
                width=usable_width,
                height=self.row_heights["overview_page"]["main_title_row"]
                * usable_height,
            ).fill(
                test_name="ramp-up",
                date=datetime.date.today(),  # TODO: Update to use actual date
            )
        )

        self.boxes["overview_page"]["metadata_title_row"].append(
            SectionTitleBox(
                page=self.document[0],
                page_margin=page_margin,
                width=usable_width,
                height=self.row_heights["overview_page"]["metadata_title_row"]
                * usable_height,
                top_padding=self.row_heights["overview_page"]["main_title_row"]
                * usable_height,
            ).fill(text="Metadata and parameters")
        )

        self.boxes["overview_page"]["metadata_row"].append(
            SectionContentBox(
                page=self.document[0],
                page_margin=page_margin,
                width=usable_width,
                height=self.row_heights["overview_page"]["metadata_row"]
                * usable_height,
                top_padding=(
                    self.row_heights["overview_page"]["main_title_row"]
                    + self.row_heights["overview_page"]["metadata_title_row"]
                )
                * usable_height,
            ).fill(
                # TODO: Add metadata content
            )
        )

        self.boxes["overview_page"]["main_graph_title_row"].append(
            SectionTitleBox(
                page=self.document[0],
                page_margin=page_margin,
                width=usable_width,
                height=self.row_heights["overview_page"]["main_graph_title_row"]
                * usable_height,
                top_padding=(
                    self.row_heights["overview_page"]["main_title_row"]
                    + self.row_heights["overview_page"]["metadata_title_row"]
                    + self.row_heights["overview_page"]["metadata_row"]
                )
                * usable_height,
            ).fill(text="Latency graphs")
        )

        self.boxes["overview_page"]["main_graph_subtitle_row"].append(
            SectionSubtitleBox(
                page=self.document[0],
                page_margin=page_margin,
                width=usable_width,
                height=self.row_heights["overview_page"]["main_graph_subtitle_row"]
                * usable_height,
                top_padding=(
                    self.row_heights["overview_page"]["main_title_row"]
                    + self.row_heights["overview_page"]["metadata_title_row"]
                    + self.row_heights["overview_page"]["metadata_row"]
                    + self.row_heights["overview_page"]["main_graph_title_row"]
                )
                * usable_height,
            ).fill(text="Comparison of all modules")
        )

        self.boxes["overview_page"]["main_graph_row"].append(
            SectionContentBox(
                page=self.document[0],
                page_margin=page_margin,
                width=usable_width,
                height=self.row_heights["overview_page"]["main_graph_row"]
                * usable_height,
                top_padding=(
                    self.row_heights["overview_page"]["main_title_row"]
                    + self.row_heights["overview_page"]["metadata_title_row"]
                    + self.row_heights["overview_page"]["metadata_row"]
                    + self.row_heights["overview_page"]["main_graph_title_row"]
                    + self.row_heights["overview_page"]["main_graph_subtitle_row"]
                )
                * usable_height,
            ).fill(
                # TODO: Add metadata content
            )
        )

        self.boxes["overview_page"]["first_detail_graphs_titles_row"].append(
            SectionTitleBox(
                page=self.document[0],
                page_margin=page_margin,
                width=usable_width,
                height=self.row_heights["overview_page"][
                    "first_detail_graphs_titles_row"
                ]
                * usable_height,
                top_padding=(
                    self.row_heights["overview_page"]["main_title_row"]
                    + self.row_heights["overview_page"]["metadata_title_row"]
                    + self.row_heights["overview_page"]["metadata_row"]
                    + self.row_heights["overview_page"]["main_graph_title_row"]
                    + self.row_heights["overview_page"]["main_graph_subtitle_row"]
                    + self.row_heights["overview_page"]["main_graph_row"]
                )
                * usable_height,
            ).fill(text="Latency graphs")
        )

    def __prepare_overview_page(self):
        page_margin = self.standard_page_margin.copy()

        usable_width = (
            self.page_width - page_margin.get("left") - page_margin.get("right")
        )
        usable_height = (
            self.page_height - page_margin.get("top") - page_margin.get("bottom")
        )

        page = self.document.new_page(  # noqa
            0,  # insertion point: begin of document
            width=self.page_width,
            height=self.page_height,
        )

        self.__prepare_overview_page_boxes()
        self.__prepare_overview_page_row_heights()

        return page_margin, usable_width, usable_height

    def __prepare_overview_page_boxes(self):
        self.boxes["overview_page"] = {
            "main_title_row": [],  # 1st row
            "metadata_title_row": [],  # 2nd row
            "metadata_row": [],  # 3rd row
            "main_graph_title_row": [],  # 4th row
            "main_graph_subtitle_row": [],  # 5th row
            "main_graph_row": [],  # 6th row
            "first_detail_graphs_titles_row": [],  # 7th row
            "first_detail_graphs_subtitles_row": [],  # 8th row
            "first_detail_graphs_row": [],  # 9th row
            "second_detail_graphs_title_row": [],  # 10th row
            "second_detail_graphs_subtitles_row": [],  # 11th row
            "second_detail_graphs_row": [],  # 12th row
        }

    def __prepare_overview_page_row_heights(self):
        self.row_heights["overview_page"] = {
            "main_title_row": 0.05,  # 1st row
            "metadata_title_row": 0.025,  # 2nd row
            "metadata_row": 0.10,  # 3rd row
            "main_graph_title_row": 0.025,  # 4th row
            "main_graph_subtitle_row": 0.025,  # 5th row
            "main_graph_row": 0.30,  # 6th row
            "first_detail_graphs_titles_row": 0.025,  # 7th row
            "first_detail_graphs_subtitles_row": 0.025,  # 8th row
            "first_detail_graphs_row": 0.05,  # 9th row
            "second_detail_graphs_title_row": 0.025,  # 10th row
            "second_detail_graphs_subtitles_row": 0.025,  # 11th row
            "second_detail_graphs_row": 0.05,  # 12th row
        }

    def save_file(self, relative_output_directory_path: Path, output_filename: str):
        """Stores the document as a file.

        Args:
            relative_output_directory_path (Path): Path to the directory in which to store the output file, relative to
                        the project base directory
            output_filename (str): Filename the output file should have, without file type
        """
        absolute_output_directory_path = BASE_DIR / relative_output_directory_path
        os.makedirs(absolute_output_directory_path, exist_ok=True)

        relative_output_filename = (
            relative_output_directory_path / f"{output_filename}.pdf"
        )
        absolute_output_filename = (
            absolute_output_directory_path / f"{output_filename}.pdf"
        )

        try:
            self.document.save(absolute_output_filename, garbage=4, deflate=True)
            logger.info(
                f"Successfully stored document under {relative_output_filename}"
            )
        except ValueError as err:  # includes zero page error
            logger.error(err)


# Only for testing
if __name__ == "__main__":
    generator = PDFOverviewGenerator()

    generator.setup_first_page_layout()

    generator.save_file(Path("testing_reports"), "report")
