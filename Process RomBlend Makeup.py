from workflow.execution.sessionmodelset import SessionModelSet
from workflow.execution.sessionrunhandle import SessionRunHandle

handle: SessionRunHandle = handle  # type: ignore
input_set: SessionModelSet = input_set  # type: ignore
output_set: SessionModelSet = output_set  # type: ignore

from libblocksmith import BlockModel  # type: ignore
from libblocksmith import TableModel  # type: ignore
import os
import pandas as pd

VALUE_COL = "Unnamed: 5"

for input_model in input_set.get_all("Input Models"):
    source_file_name = str(input_model.get_attribute("SourceFileName", ""))
    source_directory = str(input_model.get_attribute("SourceDirectory", "")) or os.getcwd()

    df = input_model.read().to_pandas()
      
    required = ["SourceFullName", "SourceParcel", "DestinationParcel", "DataField", VALUE_COL]
    missing = [c for c in required if c not in df.columns]
    if missing:
        handle.log_info(f"SKIP {source_file_name}: missing columns {missing}")
        continue

    # Filter rows where SourceFullName is not null
    df = df[df["SourceFullName"].notna()].copy()

    # Cast text columns
    df = df.astype(
        {
            "SourceFullName": "string",
            "SourceParcel": "string",
            "DestinationParcel": "string",
            "DataField": "string",
        }
    )

    # Value conversion (all imported as string)
    df[VALUE_COL] = (
        df[VALUE_COL]
        .astype(str)
        .str.replace(",", "", regex=False)
        .str.strip()
        .replace({"": None, "nan": None, "NaN": None})
    )
    df[VALUE_COL] = pd.to_numeric(df[VALUE_COL], errors="coerce").fillna(0.0)

    # Group by SourceFullName ONLY (matches your PQ join logic)
    grouped = (
        df.groupby(["SourceFullName"], as_index=False)[VALUE_COL]
        .sum()
        .rename(columns={VALUE_COL: "Tonnes"})
    )

    # Merge on SourceFullName only (matches PQ)
    merged = pd.merge(df, grouped, on=["SourceFullName"], how="left")

    # Ratio
    merged["Ratio"] = merged.apply(
        lambda r: (r[VALUE_COL] / r["Tonnes"]) if r["Tonnes"] not in (0, 0.0, None) else 0.0,
        axis=1,
    )

    # Remove DataField, Value, Tonnes
    result = merged.drop(columns=["DataField", VALUE_COL, "Tonnes"])

    # Sort
    result = result.sort_values(by="SourceFullName").reset_index(drop=True)
    
    # Replace values in SourceFullName
    result["SourceFullName"] = (
        result["SourceFullName"]
        .str.replace("OpenPit", "Reserves", regex=False)
        .str.replace("/", "_", regex=False)
    )
    
    # ---- Remove 4th column (index 3) ----
    if result.shape[1] >= 4:
        result = result.drop(columns=result.columns[3])
        
    # add source directory column for downstream script
    result["SourceDirectory"] = source_directory
    result["SourceFullName"] = df["SourceFullName"].astype(str).str.replace("Reserves", "OpenPit", regex=False)
    result["SourceFullName"] = df["SourceFullName"].astype(str).str.replace("/", "_", regex=False)
   

    # Write next to input model
    base = os.path.splitext(source_file_name)[0] if source_file_name else "input_model"
    output_path = os.path.join(source_directory, f"processed_{base}.csv")
    result.to_csv(output_path, index=False)

    handle.log_info(f"Wrote: {output_path}")

    # write output for the one matched model
    output_model = handle.create_model("Table", input_model.label.lower())
    TableModel.from_pandas(result).write(output_model.model_path)
    output_set.append_model("Output Models", output_model)
   