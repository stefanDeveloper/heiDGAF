import logging

import polars as pl

from heidgaf import ReturnCode
from heidgaf.inspectors import Inspector, InspectorConfig


class IPInspector(Inspector):
    KEY_IP_FREQUENCY = "client_ip_error_frequency"
    KEY_DNS_SERVER = "dns_server_error_frequency"

    def __init__(self, config: InspectorConfig) -> None:
        """IP inspector class. It checks for anomalies of requests by client IPs.

        Args:
            config (InspectorConfig): Inspector configuraiton.
        """
        super().__init__(config)

    def update_threshold(threshould, tpr, fpr):
        pass

    def run(self, data: pl.DataFrame) -> pl.DataFrame:
        """Runs tester for IP address based features.

        Args:
            data (pl.DataFrame): Preprocessed data.

        Returns:
            pl.DataFrame: Suspicious Ids.
        """
        min_date = data.select(["timestamp"]).min().item()
        max_date = data.select(["timestamp"]).max().item()

        # Filter data with no errors
        df = (
            data.filter(pl.col("query") != "|")
            .filter(pl.col("return_code") != ReturnCode.NOERROR.value)
            .filter(pl.col("query").str.split(".").list.len() != 1)
        )

        findings = []

        # Update frequencies based on errors
        logging.info("Analyze client IP requests anomalies")
        warnings = self.update_count(
            df, min_date, max_date, "client_ip", self.KEY_IP_FREQUENCY
        )
        findings.append(self.warnings(data, warnings, "client_ip"))

        logging.info("Analyze DNS server requests anomalies")
        warnings = self.update_count(
            df, min_date, max_date, "dns_server", self.KEY_DNS_SERVER
        )
        findings.append(self.warnings(data, warnings, "dns_server"))

        return pl.concat(findings)
