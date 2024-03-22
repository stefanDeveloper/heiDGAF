from enum import Enum
import logging

CONTEXT_SETTINGS = dict(help_option_names=["-h", "--help"], show_default=True)


class ReturnCode(Enum):
    NOERROR = "NOERROR"


# set up logging to file
logging.basicConfig(level=logging.DEBUG,
                    format='%(asctime)s %(name)-12s %(levelname)-8s %(message)s',
                    datefmt='%y-%m-%d %H:%M:%S',
                    handlers=[  
                        logging.FileHandler("heidgaf.log"),
                        logging.StreamHandler()
                    ])
