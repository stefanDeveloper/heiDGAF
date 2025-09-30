from datetime import timedelta


class SingleMetadataInformation:
    """Includes a title and an informative string value. To be used in the :cls:`SectionContentMetadataBox`.
    For other types of values, use the subclasses."""

    def __init__(self, title: str, value: str):
        self.title = title
        self.value = value


class IntegerMetadataInformation(SingleMetadataInformation):
    """Includes a title and an informative integer value."""

    def __init__(self, title: str, value: int):
        str_value = f"{value:,}"

        super().__init__(title=title, value=str_value)


class DurationMetadataInformation(SingleMetadataInformation):
    """Includes a title and an informative value, indicating duration in hours, minutes and seconds."""

    def __init__(self, title: str, value: timedelta):
        str_value = self._format_timedelta(value)

        super().__init__(title=title, value=str_value)

    @staticmethod
    def _format_timedelta(td: timedelta) -> str:
        total_seconds = int(td.total_seconds())

        hours, remainder = divmod(total_seconds, 3600)
        minutes, seconds = divmod(remainder, 60)

        parts = []
        if hours:
            parts.append(f"{hours}h")
        if minutes:
            parts.append(f"{minutes}min")
        if seconds:
            parts.append(f"{seconds}s")

        return " ".join(parts) if parts else "0 s"


class NumberPerTimeMetadataInformation(SingleMetadataInformation):
    """Includes a title and an informative value, indicating a number per time, e.g. loglines per second."""

    def __init__(self, title: str, value: float, per: str):
        """
        Args:
            title: Descriptive title of the information.
            value: Value per time, e.g. 27.4 for 27.4/s.
            per: Per time, must be "s", "min" or "h".
        """
        str_value = self._format_rate(value, per)

        super().__init__(title=title, value=str_value)

    @staticmethod
    def _format_rate(value: float, per: str = "s") -> str:
        """
        Formats a rate as 'value/unit'.

        Args:
            value: The value as float.
            per: Unit of interval. "s", "min" or "h".

        Returns:
            String in format 'value/unit', e.g. "27.4/s".
        """
        per = per.lower()
        if per == "s":
            suffix = "/s"
        elif per == "min":
            suffix = "/min"
        elif per == "h":
            suffix = "/h"
        else:
            raise ValueError("per must be one of 's', 'min', or 'h'")

        return f"{value:,.1f}{suffix}"


if __name__ == "__main__":
    sut = NumberPerTimeMetadataInformation(title="Test", value=749926, per="s")
    print(sut.title, sut.value)
