import json
import pandas as pd


def write_json(file_path, obj):
    """Writes a dictionary object to a JSON file."""
    with open(file_path, 'w') as f:
        json.dump(obj, f, indent=4)
        f.close

def read_json(file_path):
    """Reads a JSON file to a dictionary object."""
    with open(file_path, 'r') as f:
        obj = json.load(f)
        f.close()
    return obj 

def add_to_csv(file_path, content):
    """Reads a CSV file, appends DataFrame content, and
    writes back to file."""
    dataframe = pd.read_csv(file_path)
    dataframe = dataframe.append(content)

    dataframe.to_csv(file_path)