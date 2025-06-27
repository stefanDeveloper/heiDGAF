import sys
import os
import threading
import subprocess
import glob
sys.path.append(os.getcwd())
from src.base.log_config import get_logger

logger = get_logger("zeek.sensor")
class ZeekAnalysisHandler():
    def __init__(self, zeek_config_location: str, zeek_log_location: str):
        self.zeek_log_location = zeek_log_location
        self.zeek_config_location = zeek_config_location
    
    def start_analysis(self, static_analysis: bool):
        if static_analysis:
            logger.info("static analysis mode selected")
            self.start_static_analysis()
        else:
            logger.info("network analysis mode selected")
            self.start_network_analysis()
        
    def start_static_analysis(self):
        self.static_files_dir = os.getenv("STATIC_FILES_DIR")
        files = glob.glob(f"{self.static_files_dir}/*.pcap")
        threads = []
        for file in files:
            logger.info(f"Starting Analysis for file {file}...")
            command = ["zeek", "-r", file, self.zeek_config_location]
            thread = threading.Thread(target=subprocess.run, args=(command,))
            thread.start()
            threads.append(thread)
        
        for thread in threads:
            thread.join()
        logger.info("Finished static analyses")
        
    def start_network_analysis(self):
        start_zeek = ["zeekctl", "deploy"]
        thread = threading.Thread(target=subprocess.run, args=(start_zeek,))
        thread.start()
        thread.join()
           
        process = subprocess.Popen(
            ["tail", "-f", "/dev/null"],
            stdout=subprocess.PIPE,
            stderr=subprocess.STDOUT,
            text=True
        )

        def read_output():
            for line in iter(process.stdout.readline, ''):
                if line:
                    print(f"[ZEEK LOG] {line}", end='')
            process.stdout.close()
        logger.info("network analysis started")
        # Start background thread to read stdout line by line
        # necesseray because otherwise subprocess stdout will run into buffer errors eventually
        reader_thread = threading.Thread(target=read_output, daemon=True)
        reader_thread.start()
        logger.info("network analysis ongoing")
        reader_thread.join()
        logger.info("network analysis stopped")
