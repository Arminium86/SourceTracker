from workflow.execution.sessionmodelset import SessionModelSet
from workflow.execution.sessionrunhandle import SessionRunHandle

handle: SessionRunHandle = handle  # type: ignore
input_set: SessionModelSet = input_set  # type: ignore
output_set: SessionModelSet = output_set  # type: ignore

import os
import pandas as pd
from libblocksmith import TableModel  # ensure imported at top

# ---------- helpers ----------
def to_num_series(s: pd.Series) -> pd.Series:
    return pd.to_numeric(
        s.astype(str)
         .str.replace(",", "", regex=False)
         .str.strip()
         .replace({"": None, "nan": None, "NaN": None}),
        errors="coerce",
    ).fillna(0.0)

def coerce_numeric(df: pd.DataFrame, cols: list[str]) -> pd.DataFrame:
    for c in cols:
        if c in df.columns:
            df[c] = to_num_series(df[c])
    return df

def strip_cols(df: pd.DataFrame) -> pd.DataFrame:
    df.columns = [str(c).strip() for c in df.columns]
    return df


# ---------- pick the two inputs ----------
crusher_df = None
makeup_df = None

for m in input_set.get_all("Input Models"):
    d = strip_cols(m.read().to_pandas())

    if {"OriginalSourceFullName", "SourceParcel", "FinalDestinationFullName", "Mining_wetTonnes"}.issubset(d.columns):
        crusher_df = d
        continue

    if {"SourceFullName", "SourceParcel", "DestinationParcel", "Ratio", "SourceDirectory"}.issubset(d.columns):
        makeup_df = d
        continue

if crusher_df is None:
    raise ValueError("Could not find Crusher Report model (needs OriginalSourceFullName, SourceParcel, FinalDestinationFullName, Mining_wetTonnes).")
if makeup_df is None:
    raise ValueError("Could not find Romblend Makeup model (needs SourceFullName, SourceParcel, DestinationParcel, Ratio, SourceDirectory).")

# output folder from makeup table
out_dir = str(makeup_df["SourceDirectory"].dropna().iloc[0]) if makeup_df["SourceDirectory"].notna().any() else os.getcwd()

# ---------- Step A: InitialData (crusher PQ logic) ----------
df = crusher_df.copy()

prod_cols = [
    "Mining_2YProdDest_ftff","Mining_2YProdDest_ftsf","Mining_2YProdDest_ftwf",
    "Mining_2YProdDest_kgkf","Mining_2YProdDest_kgkf.cv.vq","Mining_2YProdDest_kgsf",
    "Mining_2YProdDest_kgsf.cv.vq","Mining_2YProdDest_kgwf","Mining_2YProdDest_unflagged",
    "Mining_2YProdDest_unscheduled","Mining_2YProdDest_untracked","Mining_2YProdDest_waste"
]
grade_cols = ["Mining_grades_fe", "Mining_grades_si", "Mining_grades_al", "Mining_grades_mn", "Mining_grades_p"]
ore_cols = [
    "Mining_oretype_bid","Mining_oretype_biddid","Mining_oretype_cidl","Mining_oretype_cidm",
    "Mining_oretype_cidmcidu","Mining_oretype_cidtotal","Mining_oretype_cidu","Mining_oretype_did","Mining_oretype_hcp"
]

df = coerce_numeric(df, ["Mining_wetTonnes"] + prod_cols + grade_cols + ore_cols)

bad_dests = {"Crushers/Feedable_FB", "Crushers/Feedable_FTSS", "Crushers/Feedable_KF", "Crushers/Feedable_SS"}
df = df[~df["FinalDestinationFullName"].isin(bad_dests)].copy()

df = df.drop(columns=[c for c in ["OriginalSource.OpenPit", "FinalDestination.Top"] if c in df.columns], errors="ignore")

has_period = "Period.Name" in df.columns

# weighted grades
df["fe_weighted"] = df["Mining_grades_fe"] * df["Mining_wetTonnes"]
df["si_weighted"] = df["Mining_grades_si"] * df["Mining_wetTonnes"]
df["al_weighted"] = df["Mining_grades_al"] * df["Mining_wetTonnes"]
df["mn_weighted"] = df["Mining_grades_mn"] * df["Mining_wetTonnes"]
df["p_weighted"]  = df["Mining_grades_p"]  * df["Mining_wetTonnes"]

# --- original grain (used for output model and original CSV) ---
group_cols = ["OriginalSourceFullName", "SourceParcel", "FinalDestinationFullName"]
sum_cols = ["Mining_wetTonnes"] + prod_cols + ore_cols + ["fe_weighted","si_weighted","al_weighted","mn_weighted","p_weighted"]
sum_cols = [c for c in sum_cols if c in df.columns]

initial = df.groupby(group_cols, as_index=False)[sum_cols].sum()

# back-calc grades
initial["Mining_grades_fe"] = initial["fe_weighted"] / initial["Mining_wetTonnes"].replace({0: pd.NA})
initial["Mining_grades_si"] = initial["si_weighted"] / initial["Mining_wetTonnes"].replace({0: pd.NA})
initial["Mining_grades_al"] = initial["al_weighted"] / initial["Mining_wetTonnes"].replace({0: pd.NA})
initial["Mining_grades_mn"] = initial["mn_weighted"] / initial["Mining_wetTonnes"].replace({0: pd.NA})
initial["Mining_grades_p"]  = initial["p_weighted"]  / initial["Mining_wetTonnes"].replace({0: pd.NA})
initial[grade_cols] = initial[grade_cols].fillna(0.0)
initial = initial.drop(columns=["fe_weighted","si_weighted","al_weighted","mn_weighted","p_weighted"], errors="ignore")

# --- period grain (used only for extra local CSV) ---
initial_period = None
if has_period:
    group_cols_period = ["OriginalSourceFullName", "SourceParcel", "FinalDestinationFullName", "Period.Name"]
    initial_period = df.groupby(group_cols_period, as_index=False)[sum_cols].sum()

    initial_period["Mining_grades_fe"] = initial_period["fe_weighted"] / initial_period["Mining_wetTonnes"].replace({0: pd.NA})
    initial_period["Mining_grades_si"] = initial_period["si_weighted"] / initial_period["Mining_wetTonnes"].replace({0: pd.NA})
    initial_period["Mining_grades_al"] = initial_period["al_weighted"] / initial_period["Mining_wetTonnes"].replace({0: pd.NA})
    initial_period["Mining_grades_mn"] = initial_period["mn_weighted"] / initial_period["Mining_wetTonnes"].replace({0: pd.NA})
    initial_period["Mining_grades_p"]  = initial_period["p_weighted"]  / initial_period["Mining_wetTonnes"].replace({0: pd.NA})
    initial_period[grade_cols] = initial_period[grade_cols].fillna(0.0)
    initial_period = initial_period.drop(columns=["fe_weighted","si_weighted","al_weighted","mn_weighted","p_weighted"], errors="ignore")

# ---------- Step B: Romblend Makeup prep ----------
mk = makeup_df.copy()
mk = mk[mk["SourceFullName"].notna()].copy()
mk["Ratio"] = to_num_series(mk["Ratio"])

mk = mk[["SourceFullName", "SourceParcel", "DestinationParcel", "Ratio", "SourceDirectory"]].copy()
mk["SourceFullName"] = mk["SourceFullName"].astype(str)
mk["DestinationParcel"] = mk["DestinationParcel"].astype(str)

# ---------- shared transformation function ----------
def apply_makeup_logic(initial_in: pd.DataFrame, keep_period: bool = False) -> pd.DataFrame:
    joined = initial_in.merge(
        mk,
        left_on=["OriginalSourceFullName", "SourceParcel"],
        right_on=["SourceFullName", "DestinationParcel"],
        how="left",
        suffixes=("_cr", "_mk"),
    )

    # resolve left SourceParcel column name after merge
    left_sp = "SourceParcel"
    if left_sp not in joined.columns:
        if "SourceParcel_cr" in joined.columns:
            left_sp = "SourceParcel_cr"
        elif "SourceParcel_x" in joined.columns:
            left_sp = "SourceParcel_x"
        else:
            raise KeyError(f"Could not find left SourceParcel in joined. Columns start: {list(joined.columns)[:50]}")

    # resolve mk SourceParcel column name after merge (right side)
    mk_sp = None
    for cand in ["SourceParcel_mk", "SourceParcel_y", "SourceParcel"]:
        if cand in joined.columns:
            mk_sp = cand
            break

    has_ratio = joined["Ratio"].notna()

    # Parcel tonnes
    joined["ParcelTonnesToDestination"] = joined["Mining_wetTonnes"]
    joined.loc[has_ratio, "ParcelTonnesToDestination"] = joined.loc[has_ratio, "Mining_wetTonnes"] * joined.loc[has_ratio, "Ratio"]

    # adjusted prod cols
    for c in prod_cols:
        if c in joined.columns:
            joined[f"{c}_adjusted"] = joined[c]
            joined.loc[has_ratio, f"{c}_adjusted"] = joined.loc[has_ratio, c] * joined.loc[has_ratio, "Ratio"]

    # SourceParcelUpdated: if SourceParcel == ROMBLEND then use mk.SourceParcel (if present) else keep
    joined["SourceParcelUpdated"] = joined[left_sp]
    if mk_sp is not None:
        joined.loc[joined[left_sp] == "ROMBLEND", "SourceParcelUpdated"] = joined.loc[joined[left_sp] == "ROMBLEND", mk_sp]

    # weighted grades based on ParcelTonnesToDestination
    joined["fe_weighted"] = joined["Mining_grades_fe"] * joined["ParcelTonnesToDestination"]
    joined["si_weighted"] = joined["Mining_grades_si"] * joined["ParcelTonnesToDestination"]
    joined["al_weighted"] = joined["Mining_grades_al"] * joined["ParcelTonnesToDestination"]
    joined["mn_weighted"] = joined["Mining_grades_mn"] * joined["ParcelTonnesToDestination"]
    joined["p_weighted"]  = joined["Mining_grades_p"]  * joined["ParcelTonnesToDestination"]

    # regroup
    g2 = ["OriginalSourceFullName", "SourceParcelUpdated", "FinalDestinationFullName"]
    if keep_period and "Period.Name" in joined.columns:
        g2.append("Period.Name")

    agg_dict = {
        "ParcelTonnesToDestination": "sum",
        "fe_weighted": "sum",
        "si_weighted": "sum",
        "al_weighted": "sum",
        "mn_weighted": "sum",
        "p_weighted": "sum",
    }
    for c in prod_cols:
        adj = f"{c}_adjusted"
        if adj in joined.columns:
            agg_dict[adj] = "sum"
    for c in ore_cols:
        if c in joined.columns:
            agg_dict[c] = "sum"

    agg = joined.groupby(g2).agg(agg_dict).reset_index()

    # rename tonnes + prod back
    agg = agg.rename(columns={"ParcelTonnesToDestination": "Mining_wetTonnes"})
    for c in prod_cols:
        adj = f"{c}_adjusted"
        if adj in agg.columns:
            agg = agg.rename(columns={adj: c})

    # back-calc grades
    agg["Mining_grades_fe"] = agg["fe_weighted"] / agg["Mining_wetTonnes"].replace({0: pd.NA})
    agg["Mining_grades_si"] = agg["si_weighted"] / agg["Mining_wetTonnes"].replace({0: pd.NA})
    agg["Mining_grades_al"] = agg["al_weighted"] / agg["Mining_wetTonnes"].replace({0: pd.NA})
    agg["Mining_grades_mn"] = agg["mn_weighted"] / agg["Mining_wetTonnes"].replace({0: pd.NA})
    agg["Mining_grades_p"]  = agg["p_weighted"]  / agg["Mining_wetTonnes"].replace({0: pd.NA})
    agg[grade_cols] = agg[grade_cols].fillna(0.0)
    agg = agg.drop(columns=["fe_weighted","si_weighted","al_weighted","mn_weighted","p_weighted"], errors="ignore")

    # rename parcel back
    agg = agg.rename(columns={"SourceParcelUpdated": "SourceParcel"})
    agg = agg[agg["SourceParcel"].notna()].copy()

    # replace ROMBLEND parcel using top parcel by tonnes
    rom = agg[agg["SourceParcel"] == "ROMBLEND"].copy()
    non = agg[agg["SourceParcel"] != "ROMBLEND"].copy()

    top_keys = ["OriginalSourceFullName", "FinalDestinationFullName"]
    if keep_period and "Period.Name" in non.columns:
        top_keys.append("Period.Name")

    top = (
        non.sort_values("Mining_wetTonnes", ascending=False)
           .drop_duplicates(subset=top_keys)
           [top_keys + ["SourceParcel"]]
           .rename(columns={"SourceParcel": "ReplacementParcel"})
    )

    rom2 = rom.merge(top, on=top_keys, how="left")
    rom2["SourceParcel"] = rom2["ReplacementParcel"].fillna("ROMBLEND")
    rom2 = rom2.drop(columns=["ReplacementParcel"])

    final_out = pd.concat([non, rom2], ignore_index=True)

    # Ensure numeric
    final_out["Mining_wetTonnes"] = pd.to_numeric(final_out["Mining_wetTonnes"], errors="coerce").fillna(0)

    # Filter out zero-tonne rows
    final_out = final_out[final_out["Mining_wetTonnes"] > 0].copy()

    final_out["SourceDirectory"] = str(makeup_df["SourceDirectory"].dropna().iloc[0])

    return final_out


# ---------- build outputs ----------
final = apply_makeup_logic(initial, keep_period=False)

final_period = None
if initial_period is not None:
    final_period = apply_makeup_logic(initial_period, keep_period=True)

handle.log_info(f"Rows after removing zero-tonnes: {len(final)}")
if final_period is not None:
    handle.log_info(f"Rows in period-grain export after removing zero-tonnes: {len(final_period)}")


# -----------------------------
# Export to CSVs
# -----------------------------
source_directory = str(makeup_df["SourceDirectory"].dropna().iloc[0])

output_path = os.path.join(source_directory, "crusher_interim_report.csv")
final.to_csv(output_path, index=False)

handle.log_info(f"Wrote combined CSV to: {output_path}")
handle.log_info(f"Final row count: {len(final)}")

if final_period is not None:
    output_path_period = os.path.join(source_directory, "crusher_interim_report_by_period.csv")
    final_period.to_csv(output_path_period, index=False)
    handle.log_info(f"Wrote period-grain CSV to: {output_path_period}")
    handle.log_info(f"Period-grain row count: {len(final_period)}")
else:
    handle.log_info("Period.Name not found in crusher input, so period-grain CSV was not written.")

# -----------------------------
# Output model remains unchanged
# -----------------------------
output_model = handle.create_model("Table", "crusher_interim_report")
TableModel.from_pandas(final).write(output_model.model_path)
output_set.append_model("Output Models", output_model)

handle.log_info("Output model written: crusher_interim_report")