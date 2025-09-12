import sys
import os
import threading
import subprocess
import glob

sys.path.append(os.getcwd())
from src.base.log_config import get_logger

logger = get_logger("zeek.sensor")


class ZeekAnalysisHandler:
    """
        Handles the execution of Zeek analysis in either static or network analysis mode.
        
        This class manages the Zeek processing workflow, supporting both static analysis of
        PCAP files and live network traffic analysis. It provides the necessary infrastructure
        for launching Zeek processes, managing their execution, and handling their output.
        
    """
    def __init__(self, zeek_config_location: str, zeek_log_location: str):
        """
        Initialize the Zeek analysis handler with configuration and log locations.
        
        Args:
            zeek_config_location: Path to the Zeek configuration file that defines
                the analysis scripts and plugins to be loaded
            zeek_log_location: Path where Zeek will write its processing logs
            
        Note:
            The configuration file location typically points to local.zeek or
            another site-specific configuration file that incorporates the necessary
            analysis scripts and Kafka plugin configuration.
        """
        self.zeek_log_location = zeek_log_location
        self.zeek_config_location = zeek_config_location

    def start_analysis(self, static_analysis: bool):
        """
            Start Zeek analysis in the specified mode.
            
            This method serves as the main entry point for initiating Zeek processing,
            delegating to the appropriate analysis method based on the mode parameter.
            
            Args:
                static_analysis: If True, process stored PCAP files; if False, analyze
                    live network traffic
        """
        if static_analysis:
            logger.info("static analysis mode selected")
            self.start_static_analysis()
        else:
            logger.info("network analysis mode selected")
            self.start_network_analysis()

    def start_static_analysis(self):
        """
        Start an analysis by reading in PCAP files
                
        This method:
        1. Locates all PCAP files in the directory specified by STATIC_FILES_DIR
        2. Creates a separate Zeek process for each PCAP file
        3. Runs these processes in parallel using threads
        4. Waits for all processes to complete before returning
        
        The Zeek processes use the configured analysis scripts to process the PCAP
        files and output the results to the configured destinations (typically Kafka
        via the Zeek Kafka plugin).
        """
        self.static_files_dir = os.getenv("STATIC_FILES_DIR")
        files = glob.glob(f"{self.static_files_dir}/*.pcap")
        threads = []
        for file in files:
            logger.info(f"Starting Analysis for file {file}...")
            command = ["zeek", "-C","-r", file, self.zeek_config_location]
            thread = threading.Thread(target=subprocess.run, args=(command,))
            thread.start()
            threads.append(thread)

        for thread in threads:
            thread.join()
        logger.info("Finished static analyses")

    def start_network_analysis(self):
        """
        Start Zeek in live network analysis mode.
        
        This method:
        1. Deploys the Zeek configuration using zeekctl
        2. Starts monitoring Zeek's log output in real-time
        3. Streams the processed data to the configured output destinations
        
        The method creates a dedicated thread to monitor Zeek's log output to prevent
        buffer overflow issues that would occur if the output was processed in the
        main thread. This ensures continuous processing of network traffic without
        data loss.
        """
        start_zeek = ["zeekctl", "deploy"]
        thread = threading.Thread(target=subprocess.run, args=(start_zeek,))
        thread.start()
        thread.join()

        process = subprocess.Popen(
            ["tail", "-f", "/dev/null"],
            stdout=subprocess.PIPE,
            stderr=subprocess.STDOUT,
            text=True,
        )

        def read_output(): # pragma: no cover
            for line in iter(process.stdout.readline, ""):
                if line:
                    print(f"[ZEEK LOG] {line}", end="")
            process.stdout.close()

        logger.info("network analysis started")
        # Start background thread to read stdout line by line
        # necesseray because otherwise subprocess stdout will run into buffer errors eventually
        reader_thread = threading.Thread(target=read_output, daemon=True)
        reader_thread.start()
        logger.info("network analysis ongoing")
        reader_thread.join()
        logger.info("network analysis stopped")
