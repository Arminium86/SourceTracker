# Import related models for typing help (optional)
from workflow.execution.sessionmodelset import SessionModelSet
from workflow.execution.sessionrunhandle import SessionRunHandle

handle: SessionRunHandle = handle  # type: ignore
input_set: SessionModelSet = input_set  # type: ignore
output_set: SessionModelSet = output_set  # type: ignore

from libblocksmith import TableModel  # type: ignore
import os
import pandas as pd


# ---------- helpers ----------
def strip_cols(df: pd.DataFrame) -> pd.DataFrame:
    df.columns = [str(c).strip() for c in df.columns]
    return df

def to_num_series(s: pd.Series) -> pd.Series:
    return pd.to_numeric(
        s.astype(str).str.replace(",", "", regex=False).str.strip().replace({"": None, "nan": None, "NaN": None}),
        errors="coerce",
    )

def proddest_summary(x: str) -> str:
    x = "" if x is None else str(x)
    rules = [
        ("FTFF", "FTFF"),
        ("FTSF", "FTSF"),
        ("FTWF", "FTWF"),
        ("KGKF_CV_VQ", "KGKF_CV_VQ"),
        ("KGKF", "KGKF"),
        ("KGSF_CV_VQ", "KGSF_CV_VQ"),
        ("KGSF", "KGSF"),
        ("KGWF", "KGWF"),
        ("FB_Holding", "FB_Holding"),
        ("FTSS_Holding", "FTSS_Holding"),
        ("KF_Holding", "KF_Holding"),
        ("PF_Holding", "PF_Holding"),
        ("SS_Holding", "SS_Holding"),
        ("VQ_FTSS_Holding", "VQ_FTSS_Holding"),
        ("VQ_KF_Holding", "VQ_KF_Holding"),
        ("VQ_SS_Holding", "VQ_SS_Holding"),
    ]
    for needle, label in rules:
        if needle in x:
            return label
    return "Untracked"


# ---------- find the combined crusher report table ----------
df_in = None
src_dir = None

for input_model in input_set.get_all("Input Models"):
    d = strip_cols(input_model.read().to_pandas())
    source_directory = str(d["SourceDirectory"].dropna().iloc[0])

    if {"OriginalSourceFullName", "SourceParcel", "FinalDestinationFullName", "Mining_wetTonnes"}.issubset(d.columns):
        df_in = d
        src_dir = source_directory
        break

if df_in is None:
    raise ValueError("Could not find input combined crusher report table (needs OriginalSourceFullName, SourceParcel, FinalDestinationFullName, Mining_wetTonnes).")

# ---------- conversions ----------
df = df_in.copy()
df["Mining_wetTonnes"] = to_num_series(df["Mining_wetTonnes"]).fillna(0.0)
df["OriginalSourceFullName"] = df["OriginalSourceFullName"].astype(str)
df["SourceParcel"] = df["SourceParcel"].astype(str)
df["FinalDestinationFullName"] = df["FinalDestinationFullName"].astype(str)

# ---------- 1) Grouped Rows (detail) ----------
g_detail = (
    df.groupby(["OriginalSourceFullName", "SourceParcel", "FinalDestinationFullName"], as_index=False)["Mining_wetTonnes"]
      .sum()
)

# ---------- 2) Grouped Rows1 (total per solid+parcel) ----------
g_tot = (
    g_detail.groupby(["OriginalSourceFullName", "SourceParcel"], as_index=False)["Mining_wetTonnes"]
            .sum()
            .rename(columns={"Mining_wetTonnes": "SumOfMining_wetTonnes"})
)

# ---------- 3-4) Merge + expand ----------
m = g_detail.merge(g_tot, on=["OriginalSourceFullName", "SourceParcel"], how="left")

# ---------- 5) Ratio ----------
m["2yp_prodratio"] = m.apply(
    lambda r: (r["Mining_wetTonnes"] / r["SumOfMining_wetTonnes"]) if r["SumOfMining_wetTonnes"] not in (0, 0.0, None) else pd.NA,
    axis=1
)

# ---------- rename + drop ----------
m = m.rename(columns={
    "OriginalSourceFullName": "2yp_solid",
    "SourceParcel": "2yp_parcel",
    "FinalDestinationFullName": "2yp_proddest",
})
m = m.drop(columns=["Mining_wetTonnes", "SumOfMining_wetTonnes"], errors="ignore")

# ---------- lower parcel, replace solid "/" -> "_" ----------
m["2yp_parcel"] = m["2yp_parcel"].astype(str).str.lower()
m["2yp_solid"] = m["2yp_solid"].astype(str).str.replace("/", "_", regex=False)

# ---------- solid extended .00t then swap ----------
m["2yp_solid_extended"] = m["2yp_solid"].astype(str) + ".00t"
m = m[["2yp_solid_extended", "2yp_parcel", "2yp_proddest", "2yp_prodratio"]].rename(columns={"2yp_solid_extended": "2yp_solid"})

# ---------- proddest summary + drop raw proddest ----------
m["2yp_proddest_summary"] = m["2yp_proddest"].map(proddest_summary)
m = m.drop(columns=["2yp_proddest"], errors="ignore")
m = m.rename(columns={"2yp_proddest_summary": "2yp_proddest"})
m = m[["2yp_solid", "2yp_parcel", "2yp_proddest", "2yp_prodratio"]]

# ---------- group again (sum ratio) ----------
m["2yp_prodratio"] = to_num_series(m["2yp_prodratio"]).fillna(0.0)
m2 = (
    m.groupby(["2yp_solid", "2yp_parcel", "2yp_proddest"], as_index=False)["2yp_prodratio"]
     .sum()
)

# ---------- split solid by "_" into 7 parts, then reorder to 1_2_3_4_6_5_7 ----------
parts = m2["2yp_solid"].astype(str).str.split("_", expand=True)
# Ensure 7 columns
while parts.shape[1] < 7:
    parts[parts.shape[1]] = ""

p = [parts[i] if i in parts.columns else "" for i in range(7)]
m2["2yp_solid"] = p[0] + "_" + p[1] + "_" + p[2] + "_" + p[3] + "_" + p[5] + "_" + p[4] + "_" + p[6]

# ---------- add src_dir column ----------
m2["SourceDirectory"] = src_dir

final = m2[["2yp_solid", "2yp_parcel", "2yp_proddest", "2yp_prodratio", "SourceDirectory"]]

# ---------- export CSV + output model ----------
os.makedirs(src_dir, exist_ok=True)
output_path = os.path.join(src_dir, "2yp_proddest_writeback.csv")
final.to_csv(output_path, index=False)
handle.log_info(f"Wrote: {output_path}")

output_model = handle.create_model("Table", "2yp_proddest_writeback")
TableModel.from_pandas(final).write(output_model.model_path)
output_set.append_model("Output Models", output_model)