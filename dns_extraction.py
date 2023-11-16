import fnmatch
import os
import pandas as pd
import pandas as pd

INPUT_DIR="data"

# TODO Set data
if __name__ == "__main__":
    for root, dirnames, filenames in os.walk(INPUT_DIR):
        for filename in fnmatch.filter(filenames, "*.csv"):
            match = os.path.join(root, filename)
            sub_dir = match.replace(INPUT_DIR, "")
            print(f"Load file: {INPUT_DIR}{sub_dir}")
            df = pd.read_csv(f"{INPUT_DIR}{sub_dir}", sep = ",(?![^\[\]]*(?:\])|[^()]*\))", engine ='python')