import polars as pl

from heidgaf.pre import Analyzer


class DomainAnalyzer(Analyzer):
    def __init__(self) -> None:
        super().__init__()