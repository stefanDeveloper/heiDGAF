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
        self.boxes = {}

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

        self.boxes["overview_page"] = [
            [],  # 1st content row
            [],  # 2nd content row
            [],  # 3rd content row
            [],  # 4th content row
            [],  # 5th content row
            [],  # 6th content row
            [],  # 7th content row
            [],  # 8th content row
            [],  # 9th content row
            [],  # 10th content row
        ]

        row_heights = [
            0.08,  # title row
            0.02,  # space
            0.02,  # 2nd content row
            0.02,  # 3rd content row
            0.26,  # 4th content row
            0.04,  # space
            0.02,  # 5th content row
            0.02,  # 6th content row
            0.22,  # 7th content row
            0.04,  # space
            0.02,  # 8th content row
            0.02,  # 9th content row
            0.22,  # 10th content row
        ]  # relative of usable height, top to bottom

        def __add_title_box():
            x0 = page_margin.get("left")
            y0 = page_margin.get("top")
            x1 = x0 + usable_width  # full width
            y1 = y0 + row_heights[0] * usable_height

            self.boxes["overview_page"][0].append(pymupdf.Rect(x0, y0, x1, y1))

        def __add_metadata_title_box():
            x0 = page_margin.get("left")
            y0 = page_margin.get("top") + sum(row_heights[:2]) * usable_height
            x1 = x0 + usable_width / 3  # left third
            y1 = y0 + row_heights[2] * usable_height

            self.boxes["overview_page"][1].append(pymupdf.Rect(x0, y0, x1, y1))
            page.draw_rect(
                self.boxes["overview_page"][1][0],
                fill=(0, 0, 0, 0.2),
                color=(0, 0, 0, 0.2),
                width=1,
            )

        def __add_metadata_subtitle_box():
            x0 = page_margin.get("left")
            y0 = page_margin.get("top") + sum(row_heights[:3]) * usable_height
            x1 = x0 + usable_width / 3  # left third
            y1 = y0 + row_heights[3] * usable_height

            self.boxes["overview_page"][2].append(pymupdf.Rect(x0, y0, x1, y1))
            page.draw_rect(
                self.boxes["overview_page"][2][0],
                fill=(0, 0, 0, 0.2),
                color=(0, 0, 0, 0.2),
                width=1,
            )

        def __add_metadata_box():
            x0 = page_margin.get("left")
            y0 = page_margin.get("top") + sum(row_heights[:4]) * usable_height
            x1 = x0 + usable_width / 3  # left third
            y1 = y0 + row_heights[4] * usable_height

            self.boxes["overview_page"][3].append(pymupdf.Rect(x0, y0, x1, y1))
            page.draw_rect(
                self.boxes["overview_page"][3][0], color=(0, 0, 0, 0.2), width=1
            )

        def __add_main_graph_title_box():
            x0 = page_margin.get("left") + usable_width / 3
            y0 = page_margin.get("top") + sum(row_heights[:2]) * usable_height
            x1 = x0 + (2 / 3) * usable_width  # right two thirds
            y1 = y0 + row_heights[2] * usable_height

            self.boxes["overview_page"][1].append(pymupdf.Rect(x0, y0, x1, y1))
            page.draw_rect(
                self.boxes["overview_page"][1][1],
                fill=(0, 0, 0, 0.2),
                color=(0, 0, 0, 0.2),
                width=1,
            )

        def __add_main_graph_subtitle_box():
            x0 = page_margin.get("left") + usable_width / 3
            y0 = page_margin.get("top") + sum(row_heights[:3]) * usable_height
            x1 = x0 + (2 / 3) * usable_width  # right two thirds
            y1 = y0 + row_heights[3] * usable_height

            self.boxes["overview_page"][2].append(pymupdf.Rect(x0, y0, x1, y1))
            page.draw_rect(
                self.boxes["overview_page"][2][1],
                fill=(0, 0, 0, 0.2),
                color=(0, 0, 0, 0.2),
                width=1,
            )

        def __add_main_graph_box():
            x0 = page_margin.get("left") + usable_width / 3
            y0 = page_margin.get("top") + sum(row_heights[:4]) * usable_height
            x1 = x0 + (2 / 3) * usable_width  # right two thirds
            y1 = y0 + row_heights[4] * usable_height

            self.boxes["overview_page"][3].append(pymupdf.Rect(x0, y0, x1, y1))
            page.draw_rect(
                self.boxes["overview_page"][3][1], color=(0, 0, 0, 0.2), width=1
            )

        def __add_top_left_graph_title_box():
            x0 = page_margin.get("left")
            y0 = page_margin.get("top") + sum(row_heights[:6]) * usable_height
            x1 = x0 + usable_width / 2  # left half
            y1 = y0 + row_heights[6] * usable_height

            self.boxes["overview_page"][4].append(pymupdf.Rect(x0, y0, x1, y1))
            page.draw_rect(
                self.boxes["overview_page"][4][0],
                fill=(0, 0, 0, 0.2),
                color=(0, 0, 0, 0.2),
                width=1,
            )

        def __add_top_left_graph_subtitle_box():
            x0 = page_margin.get("left")
            y0 = page_margin.get("top") + sum(row_heights[:7]) * usable_height
            x1 = x0 + usable_width / 2  # left half
            y1 = y0 + row_heights[7] * usable_height

            self.boxes["overview_page"][5].append(pymupdf.Rect(x0, y0, x1, y1))
            page.draw_rect(
                self.boxes["overview_page"][5][0],
                fill=(0, 0, 0, 0.2),
                color=(0, 0, 0, 0.2),
                width=1,
            )

        def __add_top_left_graph_box():
            x0 = page_margin.get("left")
            y0 = page_margin.get("top") + sum(row_heights[:8]) * usable_height
            x1 = x0 + usable_width / 2  # left half
            y1 = y0 + row_heights[8] * usable_height

            self.boxes["overview_page"][6].append(pymupdf.Rect(x0, y0, x1, y1))
            page.draw_rect(
                self.boxes["overview_page"][6][0], color=(0, 0, 0, 0.2), width=1
            )

        def __add_top_right_graph_title_box():
            x0 = page_margin.get("left") + usable_width / 2
            y0 = page_margin.get("top") + sum(row_heights[:6]) * usable_height
            x1 = x0 + usable_width / 2  # right half
            y1 = y0 + row_heights[6] * usable_height

            self.boxes["overview_page"][4].append(pymupdf.Rect(x0, y0, x1, y1))
            page.draw_rect(
                self.boxes["overview_page"][4][1],
                fill=(0, 0, 0, 0.2),
                color=(0, 0, 0, 0.2),
                width=1,
            )

        def __add_top_right_graph_subtitle_box():
            x0 = page_margin.get("left") + usable_width / 2
            y0 = page_margin.get("top") + sum(row_heights[:7]) * usable_height
            x1 = x0 + usable_width / 2  # right half
            y1 = y0 + row_heights[7] * usable_height

            self.boxes["overview_page"][5].append(pymupdf.Rect(x0, y0, x1, y1))
            page.draw_rect(
                self.boxes["overview_page"][5][1],
                fill=(0, 0, 0, 0.2),
                color=(0, 0, 0, 0.2),
                width=1,
            )

        def __add_top_right_graph_box():
            x0 = page_margin.get("left") + usable_width / 2
            y0 = page_margin.get("top") + sum(row_heights[:8]) * usable_height
            x1 = x0 + usable_width / 2  # right half
            y1 = y0 + row_heights[8] * usable_height

            self.boxes["overview_page"][6].append(pymupdf.Rect(x0, y0, x1, y1))
            page.draw_rect(
                self.boxes["overview_page"][6][1], color=(0, 0, 0, 0.2), width=1
            )

        def __add_bottom_left_graph_title_box():
            x0 = page_margin.get("left")
            y0 = page_margin.get("top") + sum(row_heights[:10]) * usable_height
            x1 = x0 + usable_width  # caution: uses full width to show long title
            y1 = y0 + row_heights[10] * usable_height

            self.boxes["overview_page"][7].append(pymupdf.Rect(x0, y0, x1, y1))
            page.draw_rect(
                self.boxes["overview_page"][7][0],
                fill=(0, 0, 0, 0.2),
                color=(0, 0, 0, 0.2),
                width=1,
            )

        def __add_bottom_left_graph_subtitle_box():
            x0 = page_margin.get("left")
            y0 = page_margin.get("top") + sum(row_heights[:11]) * usable_height
            x1 = x0 + usable_width / 2  # left half
            y1 = y0 + row_heights[11] * usable_height

            self.boxes["overview_page"][8].append(pymupdf.Rect(x0, y0, x1, y1))
            page.draw_rect(
                self.boxes["overview_page"][8][0],
                fill=(0, 0, 0, 0.2),
                color=(0, 0, 0, 0.2),
                width=1,
            )

        def __add_bottom_left_graph_box():
            x0 = page_margin.get("left")
            y0 = page_margin.get("top") + sum(row_heights[:12]) * usable_height
            x1 = x0 + usable_width / 2  # left half
            y1 = y0 + row_heights[12] * usable_height

            self.boxes["overview_page"][9].append(pymupdf.Rect(x0, y0, x1, y1))
            page.draw_rect(
                self.boxes["overview_page"][9][0], color=(0, 0, 0, 0.2), width=1
            )

        def __add_bottom_right_graph_title_box():
            x0 = page_margin.get("left") + usable_width / 2
            y0 = page_margin.get("top") + sum(row_heights[:10]) * usable_height
            x1 = x0 + usable_width / 2  # right half
            y1 = y0 + row_heights[10] * usable_height

            self.boxes["overview_page"][7].append(pymupdf.Rect(x0, y0, x1, y1))
            page.draw_rect(
                self.boxes["overview_page"][7][1],
                fill=(0, 0, 0, 0.2),
                color=(0, 0, 0, 0.2),
                width=1,
            )

        def __add_bottom_right_graph_subtitle_box():
            x0 = page_margin.get("left") + usable_width / 2
            y0 = page_margin.get("top") + sum(row_heights[:11]) * usable_height
            x1 = x0 + usable_width / 2  # right half
            y1 = y0 + row_heights[11] * usable_height

            self.boxes["overview_page"][8].append(pymupdf.Rect(x0, y0, x1, y1))
            page.draw_rect(
                self.boxes["overview_page"][8][1],
                fill=(0, 0, 0, 0.2),
                color=(0, 0, 0, 0.2),
                width=1,
            )

        def __add_bottom_right_graph_box():
            x0 = page_margin.get("left") + usable_width / 2
            y0 = page_margin.get("top") + sum(row_heights[:12]) * usable_height
            x1 = x0 + usable_width / 2  # right half
            y1 = y0 + row_heights[12] * usable_height

            self.boxes["overview_page"][9].append(pymupdf.Rect(x0, y0, x1, y1))
            page.draw_rect(
                self.boxes["overview_page"][9][1], color=(0, 0, 0, 0.2), width=1
            )

        __add_title_box()
        __add_metadata_title_box()
        __add_metadata_subtitle_box()
        __add_metadata_box()
        __add_main_graph_title_box()
        __add_main_graph_subtitle_box()
        __add_main_graph_box()
        __add_top_left_graph_title_box()
        __add_top_left_graph_subtitle_box()
        __add_top_left_graph_box()
        __add_top_right_graph_title_box()
        __add_top_right_graph_subtitle_box()
        __add_top_right_graph_box()
        __add_bottom_left_graph_title_box()
        __add_bottom_left_graph_subtitle_box()
        __add_bottom_left_graph_box()
        __add_bottom_right_graph_title_box()
        __add_bottom_right_graph_subtitle_box()
        __add_bottom_right_graph_box()

    def insert_title(self):
        """Inserts the title into the existing first box."""
        title = "Ramp-Up Test"
        subtitle = "Benchmarking Report"

        page = self.document[0]  # first page
        title_box = self.boxes.get("overview_page")[0][0]

        page.insert_htmlbox(
            title_box,
            title,
            css="* {font-weight: bold; font-size: 16px; text-align: center; padding: 5px 0}",
        )
        page.insert_htmlbox(
            title_box,
            subtitle,
            css="* {"
            "font-weight: bold; font-style: italic; font-size: 12px;"
            "text-align: center; padding: 16px 0}",
        )

    def insert_box_titles(self):
        """Inserts the titles and subtitles for each box."""
        page = self.document[0]  # first page
        title_css = "* {font-size: 10px; text-align: left; padding: 1px 2px}"
        subtitle_css = "* {font-size: 7px; text-align: left; padding: 1px 2px}"

        def __add_metadata_title(text: str, subtext: str):
            box = self.boxes.get("overview_page")[1][0]
            page.insert_htmlbox(box, text, css=title_css)

            box = self.boxes.get("overview_page")[2][0]
            page.insert_htmlbox(box, subtext, css=subtitle_css)

        def __add_main_graph_title(text: str, subtext: str):
            box = self.boxes.get("overview_page")[1][1]
            page.insert_htmlbox(box, text, css=title_css)

            box = self.boxes.get("overview_page")[2][1]
            page.insert_htmlbox(box, subtext, css=subtitle_css)

        def __add_top_left_graph_title(text: str, subtext: str):
            box = self.boxes.get("overview_page")[4][0]
            page.insert_htmlbox(box, text, css=title_css)

            box = self.boxes.get("overview_page")[5][0]
            page.insert_htmlbox(box, subtext, css=subtitle_css)

        def __add_top_right_graph_title(text: str, subtext: str):
            box = self.boxes.get("overview_page")[4][1]
            page.insert_htmlbox(box, text, css=title_css)

            box = self.boxes.get("overview_page")[5][1]
            page.insert_htmlbox(box, subtext, css=subtitle_css)

        def __add_bottom_left_graph_title(text: str, subtext: str):
            box = self.boxes.get("overview_page")[7][0]
            page.insert_htmlbox(box, text, css=title_css)

            box = self.boxes.get("overview_page")[8][0]
            page.insert_htmlbox(box, subtext, css=subtitle_css)

        def __add_bottom_right_graph_title(text: str, subtext: str):
            box = self.boxes.get("overview_page")[7][1]
            page.insert_htmlbox(box, text, css=title_css)

            box = self.boxes.get("overview_page")[8][1]
            page.insert_htmlbox(box, subtext, css=subtitle_css)

        __add_metadata_title("Metadata", "Additional parameters")
        __add_main_graph_title("Latencies", "Comparison of all modules")
        __add_top_left_graph_title(
            "Latency Comparison (Boxplot)", "Min/max/median per module"
        )
        __add_top_right_graph_title("Fill Levels", "Comparison of all modules")
        __add_bottom_left_graph_title(
            "Total number of incoming and completely processed entries",
            "Sum of entries until point in time",
        )
        __add_bottom_right_graph_title("", "Entries per time")

    def insert_main_graph(self, file_name: str):
        """Inserts the main graph plot into the box."""
        page = self.document[0]  # first page
        main_graph_box = self.boxes.get("overview_page")[3][1]
        page.insert_image(self.__get_padded_rect(main_graph_box, 2), filename=f"graphs/{file_name}")

    def save_file(self):
        """Stores the document as a file."""
        file_path_and_name = os.path.join(self.output_file_path, self.output_file_name)
        os.makedirs(self.output_file_path, exist_ok=True)

        try:
            self.document.save(f"{file_path_and_name}.pdf")
            logger.info(f"Successfully stored document as {file_path_and_name}.pdf")
        except ValueError as err:  # includes zero page error
            logger.error(err)

    @staticmethod
    def __get_padded_rect(rect, padding):
        return pymupdf.Rect(
            rect.x0 + padding,
            rect.y0 + padding,
            rect.x1 - padding,
            rect.y1 - padding
        )


# Only for testing
if __name__ == "__main__":
    generator = PDFOverviewGenerator()

    generator.setup_first_page_layout()
    generator.insert_title()
    generator.insert_box_titles()
    generator.insert_main_graph("latencies_comparison.png")

    generator.save_file()
