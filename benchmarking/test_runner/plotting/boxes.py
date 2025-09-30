import datetime
from abc import abstractmethod
from pathlib import Path
from typing import Optional

import pymupdf


class BaseBox(pymupdf.Rect):
    """Base class for layout boxes to be used in PDFs."""

    def __init__(
        self,
        page,
        page_margin: dict[str, float],
        width: float,
        height: float,
        top_padding: float = 0,
        left_padding: float = 0,
    ):
        self.page = page

        x0 = page_margin.get("left") + left_padding
        y0 = page_margin.get("top") + top_padding
        x1 = x0 + width
        y1 = y0 + height

        super().__init__(x0, y0, x1, y1)

    @abstractmethod
    def fill(self, *args) -> pymupdf.Rect:
        """Fills the box with content."""
        raise NotImplementedError

    def _get_padded(self, horizontal_padding: int = 8, vertical_padding: int = 3):
        """
        Returns the same rectangle but with inner padding.

        Args:
            horizontal_padding (int): Padding in horizontal direction; default: 8
            vertical_padding (int): Padding in vertical direction; default: 3
        """
        return pymupdf.Rect(
            self.x0 + horizontal_padding,
            self.y0 + vertical_padding,
            self.x1 - horizontal_padding,
            self.y1 - vertical_padding,
        )


class MainTitleBox(BaseBox):
    """Contains the main title of a page, consisting of the test name and the date."""

    def fill(self, test_name: str, date: datetime.date):
        self.page.draw_rect(self, fill=(0,), fill_opacity=0.1, width=0.5)  # border
        self.page.insert_htmlbox(  # title
            self._get_padded(),
            f"{test_name.title()} Benchmark Test",
            css="* {font-family: sans-serif; font-size: 13px}",
        )
        self.page.insert_htmlbox(  # subtitle
            self._get_padded(),
            "Benchmarking Report",
            css="* {"
            "font-family: sans-serif; font-size: 8px; font-weight: bold;"
            "padding: 8px 0}",
        )
        self.page.insert_htmlbox(  # date
            self._get_padded(),
            str(date),
            css="* {font-family: sans-serif; font-size: 13px; text-align: right}",
        )

        return self


class SectionTitleBox(BaseBox):
    """Contains the section title."""

    def fill(self, text: str):
        self.page.draw_rect(self, fill=(0,), fill_opacity=0.3, width=0.5)  # border
        self.page.insert_htmlbox(  # title
            self._get_padded(vertical_padding=4),
            text,
            css="* {font-family: sans-serif; font-size: 8px}",
        )

        return self


class SectionDoubleTitleBox(BaseBox):
    """Contains the section titles for double-column sections."""

    def fill(self, text_1: str, text_2: str):
        self.page.draw_rect(self, fill=(0,), fill_opacity=0.3, width=0.5)  # border

        width = self.x1 - self.x0
        horizontal_padding = 8
        vertical_padding = 4
        self.page.insert_htmlbox(  # first title
            pymupdf.Rect(
                x0=self.x0 + horizontal_padding,
                y0=self.y0 + vertical_padding,
                x1=(self.x1 / 2) - horizontal_padding,
                y1=self.y1 - vertical_padding,
            ),
            text_1,
            css="* {font-family: sans-serif; font-size: 8px}",
        )
        self.page.insert_htmlbox(  # second title
            pymupdf.Rect(
                x0=self.x0 + (width / 2) + horizontal_padding,
                y0=self.y0 + vertical_padding,
                x1=self.x1 - horizontal_padding,
                y1=self.y1 - vertical_padding,
            ),
            text_2,
            css="* {font-family: sans-serif; font-size: 8px}",
        )

        return self


class SectionSubtitleBox(BaseBox):
    """Contains the section subtitle."""

    def fill(self, text: str):
        self.page.draw_rect(self, fill=(0,), fill_opacity=0.1, width=0.5)  # border
        self.page.insert_htmlbox(  # subtitle
            self._get_padded(vertical_padding=4),
            text,
            css="* {font-family: sans-serif; font-size: 7px; font-style: italic}",
        )

        return self


class SectionDoubleSubtitleBox(BaseBox):
    """Contains the section subtitles for double-column sections."""

    def fill(self, text_1: str, text_2: str):
        width = self.x1 - self.x0
        self.page.draw_rect(
            pymupdf.Rect(  # first border
                x0=self.x0,
                y0=self.y0,
                x1=self.x1 - (width / 2),
                y1=self.y1,
            ),
            fill=(0,),
            fill_opacity=0.1,
            width=0.5,
        )
        self.page.draw_rect(
            pymupdf.Rect(  # second border
                x0=self.x0 + (width / 2),
                y0=self.y0,
                x1=self.x1,
                y1=self.y1,
            ),
            fill=(0,),
            fill_opacity=0.1,
            width=0.5,
        )

        horizontal_padding = 8
        vertical_padding = 4
        self.page.insert_htmlbox(  # first subtitle
            pymupdf.Rect(
                x0=self.x0 + horizontal_padding,
                y0=self.y0 + vertical_padding,
                x1=self.x1 - (width / 2) - horizontal_padding,
                y1=self.y1 - vertical_padding,
            ),
            text_1,
            css="* {font-family: sans-serif; font-size: 7px; font-style: italic}",
        )
        self.page.insert_htmlbox(  # second subtitle
            pymupdf.Rect(
                x0=self.x0 + (width / 2) + horizontal_padding,
                y0=self.y0 + vertical_padding,
                x1=self.x1 - horizontal_padding,
                y1=self.y1 - vertical_padding,
            ),
            text_2,
            css="* {font-family: sans-serif; font-size: 7px; font-style: italic}",
        )

        return self


class SectionContentBox(BaseBox):
    """Contains the section content."""

    def fill(self, file_path: Optional[Path] = None):
        self.page.draw_rect(self, width=0.5)  # border
        if file_path is not None:  # content
            self.page.insert_image(
                self._get_padded(vertical_padding=4),
                filename=file_path,
            )

        return self
