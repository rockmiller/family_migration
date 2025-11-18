from pathlib import Path
import sys

# Add the src directory to the Python path
src_path = Path(r"C:\Users\rockm\Documents\family_migration\src")
sys.path.append(str(src_path))

# Now you can import using the full module path
from censusdb.ingest import *

#set the list of files to process
filelist = ['cs1850.csv.gz']

# execute the ingestion for all the files in the filelist
for filename in filelist:
    config = build_config(filename)
    execute_one_year(filename,config)





