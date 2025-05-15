import os.path
import sys

import pymupdf

sys.path.append(os.getcwd())
from src.base.log_config import get_logger

logger = get_logger()


class PDFOverviewGenerator:
    """Combines multiple plots and test information in a PDF document."""

    def __init__(self):
        self.output_file_path = "./output/"
        self.output_file_name = "test_for_now"

        self.page_width, self.page_height = 595, 842  # page dimension: A4 portrait
        self.standard_page_margin = {"left": 50, "right": 50, "top": 50, "bottom": 80}

        self.document = pymupdf.open()

    def setup_first_page_layout(self):
        """Adds the first page and configures its layout."""
        page_margin = self.standard_page_margin.copy()

        usable_width = (
            self.page_width - page_margin.get("left") - page_margin.get("right")
        )
        usable_height = (
            self.page_height - page_margin.get("top") - page_margin.get("bottom")
        )

        page = self.document.new_page(
            0,  # insertion point: begin of document
            width=self.page_width,
            height=self.page_height,
        )

        row_heights = [
            0.08,
            0.02,
            0.30,
            0.04,
            0.04,
            0.22,
            0.04,
            0.04,
            0.22,
        ]  # relative of usable height, top to bottom

        def __add_title_box():
            x0 = page_margin.get("left")
            y0 = page_margin.get("top")
            x1 = x0 + usable_width  # full width
            y1 = y0 + row_heights[0] * usable_height

            rect = pymupdf.Rect(x0, y0, x1, y1)
            page.draw_rect(rect, color=(0, 0, 0), width=0.5)  # TODO: Remove

        def __add_metadata_box():
            x0 = page_margin.get("left")
            y0 = page_margin.get("top") + sum(row_heights[:2]) * usable_height
            x1 = x0 + usable_width / 3  # left third
            y1 = y0 + row_heights[2] * usable_height

            rect = pymupdf.Rect(x0, y0, x1, y1)
            page.draw_rect(rect, color=(0, 0, 0), width=0.5)  # TODO: Remove

        def __add_main_graph_box():
            x0 = page_margin.get("left") + usable_width / 3
            y0 = page_margin.get("top") + sum(row_heights[:2]) * usable_height
            x1 = x0 + (2 / 3) * usable_width  # right two thirds
            y1 = y0 + row_heights[2] * usable_height

            rect = pymupdf.Rect(x0, y0, x1, y1)
            page.draw_rect(rect, color=(0, 0, 0), width=0.5)  # TODO: Remove

        def __add_top_left_graph_title_box():
            x0 = page_margin.get("left")
            y0 = page_margin.get("top") + sum(row_heights[:4]) * usable_height
            x1 = x0 + usable_width / 2  # left half
            y1 = y0 + row_heights[4] * usable_height

            rect = pymupdf.Rect(x0, y0, x1, y1)
            page.draw_rect(rect, color=(0, 0, 0), width=0.5)  # TODO: Remove

        def __add_top_right_graph_title_box():
            x0 = page_margin.get("left") + usable_width / 2
            y0 = page_margin.get("top") + sum(row_heights[:4]) * usable_height
            x1 = x0 + usable_width / 2  # right half
            y1 = y0 + row_heights[4] * usable_height

            rect = pymupdf.Rect(x0, y0, x1, y1)
            page.draw_rect(rect, color=(0, 0, 0), width=0.5)  # TODO: Remove

        def __add_top_left_graph_box():
            x0 = page_margin.get("left")
            y0 = page_margin.get("top") + sum(row_heights[:5]) * usable_height
            x1 = x0 + usable_width / 2  # left half
            y1 = y0 + row_heights[5] * usable_height

            rect = pymupdf.Rect(x0, y0, x1, y1)
            page.draw_rect(rect, color=(0, 0, 0), width=0.5)  # TODO: Remove

        def __add_top_right_graph_box():
            x0 = page_margin.get("left") + usable_width / 2
            y0 = page_margin.get("top") + sum(row_heights[:5]) * usable_height
            x1 = x0 + usable_width / 2  # right half
            y1 = y0 + row_heights[5] * usable_height

            rect = pymupdf.Rect(x0, y0, x1, y1)
            page.draw_rect(rect, color=(0, 0, 0), width=0.5)  # TODO: Remove

        def __add_bottom_left_graph_title_box():
            x0 = page_margin.get("left")
            y0 = page_margin.get("top") + sum(row_heights[:7]) * usable_height
            x1 = x0 + usable_width / 2  # left half
            y1 = y0 + row_heights[7] * usable_height

            rect = pymupdf.Rect(x0, y0, x1, y1)
            page.draw_rect(rect, color=(0, 0, 0), width=0.5)  # TODO: Remove

        def __add_bottom_right_graph_title_box():
            x0 = page_margin.get("left") + usable_width / 2
            y0 = page_margin.get("top") + sum(row_heights[:7]) * usable_height
            x1 = x0 + usable_width / 2  # right half
            y1 = y0 + row_heights[7] * usable_height

            rect = pymupdf.Rect(x0, y0, x1, y1)
            page.draw_rect(rect, color=(0, 0, 0), width=0.5)  # TODO: Remove

        def __add_bottom_left_graph_box():
            x0 = page_margin.get("left")
            y0 = page_margin.get("top") + sum(row_heights[:8]) * usable_height
            x1 = x0 + usable_width / 2  # left half
            y1 = y0 + row_heights[8] * usable_height

            rect = pymupdf.Rect(x0, y0, x1, y1)
            page.draw_rect(rect, color=(0, 0, 0), width=0.5)  # TODO: Remove

        def __add_bottom_right_graph_box():
            x0 = page_margin.get("left") + usable_width / 2
            y0 = page_margin.get("top") + sum(row_heights[:8]) * usable_height
            x1 = x0 + usable_width / 2  # right half
            y1 = y0 + row_heights[8] * usable_height

            rect = pymupdf.Rect(x0, y0, x1, y1)
            page.draw_rect(rect, color=(0, 0, 0), width=0.5)  # TODO: Remove

        __add_title_box()
        __add_metadata_box()
        __add_main_graph_box()
        __add_top_left_graph_title_box()
        __add_top_left_graph_box()
        __add_top_right_graph_title_box()
        __add_top_right_graph_box()
        __add_bottom_left_graph_title_box()
        __add_bottom_left_graph_box()
        __add_bottom_right_graph_title_box()
        __add_bottom_right_graph_box()

    def save_file(self):
        """Stores the document as a file."""
        file_path_and_name = os.path.join(self.output_file_path, self.output_file_name)
        os.makedirs(self.output_file_path, exist_ok=True)

        try:
            self.document.save(f"{file_path_and_name}.pdf")
            logger.info(f"Successfully stored document as {file_path_and_name}.pdf")
        except ValueError as err:  # includes zero page error
            logger.error(err)


# Only for testing
if __name__ == "__main__":
    generator = PDFOverviewGenerator()

    generator.setup_first_page_layout()
    generator.save_file()
