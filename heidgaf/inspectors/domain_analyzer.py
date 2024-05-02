import polars as pl

from heidgaf.inspectors import Inspector, InspectorConfig


class DomainInspector(Inspector):
    """Inspects domains based features.

    Args:
        Tester (Tester): Configuration.
    """
    KEY_SECOND_LEVEL_DOMAIN = "secondleveldomain_frequency"
    KEY_THIRD_LEVEL_DOMAIN = "thirdleveldomain_frequency"
    KEY_FQDN = "fqdn_frequency"

    def __init__(self, config: InspectorConfig) -> None:
        """Domain inspector class. It checks for anomalies of requests.

        Args:
            config (InspectorConfig): Inspector configuraiton.
        """
        super().__init__(config)

    def update_threshold(threshould, tpr, fpr):
        pass

    def run(self, data: pl.DataFrame) -> pl.DataFrame:
        """Runs tester for domain name based features.

        Args:
            data (pl.DataFrame): Proprocessed data.

        Returns:
            pl.DataFrame: Anomalies.
        """
        min_date = data.select(["timestamp"]).min().item()
        max_date = data.select(["timestamp"]).max().item()
        
        # Filter data with no errors
        df = data.filter(pl.col("query") != "|").filter(
            pl.col("query").str.split(".").list.len() != 1
        )
        
        findings = []
        
        # Check anomalies in FQDN
        warnings = self.update_count(df, min_date, max_date, "fqdn", self.KEY_FQDN)
        findings.append(self.warnings(data, warnings, "fqdn"))

        # Check anomalies in second level
        warnings = self.update_count(
            df, min_date, max_date, "secondleveldomain", self.KEY_SECOND_LEVEL_DOMAIN
        )
        findings.append(self.warnings(data, warnings, "secondleveldomain"))

        # Check anomalies in third level
        warnings = self.update_count(
            df, min_date, max_date, "thirdleveldomain", self.KEY_THIRD_LEVEL_DOMAIN
        )
        findings.append(self.warnings(data, warnings, "thirdleveldomain"))
        
        return pl.concat(findings)
