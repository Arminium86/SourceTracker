# Import related models for typing help (optional)
from workflow.execution.sessionmodelset import SessionModelSet
from workflow.execution.sessionrunhandle import SessionRunHandle

handle: SessionRunHandle = handle  # type: ignore
input_set: SessionModelSet = input_set  # type: ignore
output_set: SessionModelSet = output_set  # type: ignore

from libblocksmith import BlockModel  # type: ignore
from workflow.execution.tags import tag
import pandas as pd
import numpy as np
from libblocksmith import TableModel
from datetime import datetime
import os
import re


# Initialize variables
df_writeback = None
df_blockmodel = None
pivoted = None
writeback_path = None

# Loop through input models
for input_model in input_set.get_all("Input Models"):
    df = input_model.read().to_pandas()

    if "writeback" in str(input_model.label):
        df_writeback = df.copy()

        # Get source directory from the writeback input model
        if "SourceDirectory" not in df_writeback.columns or df_writeback["SourceDirectory"].dropna().empty:
            raise ValueError("Writeback input model is missing SourceDirectory.")
        writeback_path = str(df_writeback["SourceDirectory"].dropna().iloc[0])

        # Prepare writeback data by expanding the columns
        df_writeback["RowIndex"] = df_writeback.groupby(["2yp_solid", "2yp_parcel"]).cumcount() + 1

        melted = df_writeback.melt(
            id_vars=["2yp_solid", "2yp_parcel", "RowIndex"],
            value_vars=["2yp_proddest", "2yp_prodratio"],
            var_name="Attribute",
            value_name="Value"
        )

        melted["CombinedAttribute"] = (
            melted["Attribute"] + melted["RowIndex"].astype(str)
        )

        pivoted = melted.pivot_table(
            index=["2yp_solid", "2yp_parcel"],
            columns="CombinedAttribute",
            values="Value",
            aggfunc="first"
        ).reset_index()

        pivoted.columns.name = None
        pivoted.columns = [str(c) for c in pivoted.columns]

if df_writeback is None or pivoted is None or writeback_path is None:
    raise ValueError("Could not find a valid writeback input model.")

# Loop through input models again
for input_model in input_set.get_all("Input Models"):
    label = str(input_model.label).strip()
    
    match = re.search(r"Partitioned Copy of '([^']+)'", label)
    if match:
        source_file_name = match.group(1)

    df = input_model.read().to_pandas()

    if "writeback" in str(input_model.label):
        continue

    blockmodel_source_file_name = source_file_name
    file_name = blockmodel_source_file_name
    df_blockmodel = df.copy()

    # Perform a left join
    df_result = pd.merge(
        df_blockmodel,
        pivoted,
        how="left",
        left_on=["2yp_solid", "s_ore_type"],
        right_on=["2yp_solid", "2yp_parcel"]
    )

    for col in df_result.columns:
        if col.startswith("2yp_prodratio"):
            x = col.replace("2yp_prodratio", "")
            new_col_name = f"proddest{x}_i_t"
            df_result[new_col_name] = df_result["i_t"] * pd.to_numeric(df_result[col], errors="coerce").fillna(0)

    # Save the processed DataFrame to a CSV file
    os.makedirs(writeback_path, exist_ok=True)
    output_file = os.path.join(writeback_path, f"{file_name}.csv")
    df_result.to_csv(output_file, index=False)
    handle.log_info(f"Wrote: {output_file}")