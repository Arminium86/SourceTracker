# Import related models for typing help (optional)
from workflow.execution.sessionmodelset import SessionModelSet
from workflow.execution.sessionrunhandle import SessionRunHandle

handle: SessionRunHandle = handle # type: ignore
input_set: SessionModelSet = input_set # type: ignore
output_set: SessionModelSet = output_set # type: ignore

import os
import json
import pandas as pd

# ======== PASTE YOUR FULL CSV PATH HERE ========
CSV_PATH = r"C:\MyFiles\01 - Projects\05 - Plan Destination Guidance\Blocksmith standard version\2YP vs 3MP guidance\FY26Q3 - Jan 3MP\Working directory\Jan 3MP Feedable.Physicals.csv"

# ===============================================

KEYWORDS = ["sourcetracker", "source", "destination", "oretype", "grades"]
EXACT_COLUMNS = {"Mining_wetTonnes", "Period.Name"}

def keep_column(col: str) -> bool:
    if col in EXACT_COLUMNS:
        return True
    c = col.lower()
    return any(k in c for k in KEYWORDS)


if not os.path.isfile(CSV_PATH):
    raise FileNotFoundError(f"CSV not found: {CSV_PATH}")

# Read headers only
df = pd.read_csv(CSV_PATH, nrows=0)
headers = list(df.columns)

# Filter columns
kept_columns = [h for h in headers if keep_column(h)]

source_dir = os.path.dirname(CSV_PATH)
filename = os.path.basename(CSV_PATH)

config = {
    "SourceDirectory": source_dir,
    "Recursive": False,
    "SourceFilePatternRules": [
        {
            "Pattern": filename,
            "AttributeAssignments": "",
            "HeaderRows": 1
        }
    ],
    "SourceFilePatterns": None,
    "ThrowErrorIfNoFiles": False,
    "Columns": []
}

for col in kept_columns:
    config["Columns"].append({
        "Use": True,
        "Name": col,
        "Hidden": False,
        "Axis": 0,
        "FormatString": None,
        "WeightColumnName": None,
        "Type": "String",
        "AggregationType": "MostFrequent",
        "AlternateNamesCsv": None
    })

output_path = os.path.join(source_dir, "config.json")

with open(output_path, "w", encoding="utf-8") as f:
    json.dump(config, f, indent=2)

print(f"Config written to: {output_path}")
print(f"Columns included: {len(kept_columns)} / {len(headers)}")

