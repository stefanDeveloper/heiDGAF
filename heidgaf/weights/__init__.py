import logging

CONTEXT_SETTINGS = dict(help_option_names=["-h", "--help"], show_default=True)


# set up logging to file
logging.basicConfig(level=logging.INFO,
                    format='%(asctime)s %(name)-12s %(levelname)-8s %(message)s',
                    datefmt='%y-%m-%d %H:%M:%S',
                    handlers=[  
                        logging.FileHandler("heidgaf.log"),
                        logging.StreamHandler()
                    ])
