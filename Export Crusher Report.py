# Import related models for typing help (optional)
from workflow.execution.sessionmodelset import SessionModelSet
from workflow.execution.sessionrunhandle import SessionRunHandle

handle: SessionRunHandle = handle  # type: ignore
input_set: SessionModelSet = input_set  # type: ignore
output_set: SessionModelSet = output_set  # type: ignore

from libblocksmith import TableModel  # type: ignore
import os
import pandas as pd

REQUIRED_COLS = {
    "Destination.Top",
    "Period.Name",
    "SourceParcel",
    "FinalDestinationFullName",
    "FinalDestination.Top",
    "OriginalSource.OpenPit",
    "Mining_wetTonnes",
}

ORETYPE_KEYS = [
    "Mining_oretype_bid","Mining_oretype_biddid","Mining_oretype_cidl","Mining_oretype_cidm",
    "Mining_oretype_cidmcidu","Mining_oretype_cidtotal","Mining_oretype_cidu",
    "Mining_oretype_did","Mining_oretype_hcp"
]
GRADE_COLS = ["Mining_grades_fe", "Mining_grades_si", "Mining_grades_al", "Mining_grades_mn", "Mining_grades_p"]
PROD_KEYS = [
    "Mining_2YProdDest_ftff","Mining_2YProdDest_ftsf","Mining_2YProdDest_ftwf",
    "Mining_2YProdDest_kgkf","Mining_2YProdDest_kgkf.cv.vq","Mining_2YProdDest_kgsf",
    "Mining_2YProdDest_kgsf.cv.vq","Mining_2YProdDest_kgwf","Mining_2YProdDest_unflagged",
    "Mining_2YProdDest_unscheduled","Mining_2YProdDest_untracked","Mining_2YProdDest_waste"
]

# ---------- robust converters (everything arrives as string) ----------
BAD_TOKENS = {"", "nan", "NaN", "None", "<TOTAL>", "<total>", "TOTAL", "NULL", "null"}

def _safe_str(x) -> str:
    return "" if x is None else str(x)

def _to_num_series(s: pd.Series) -> pd.Series:
    # handles commas, <TOTAL>, blanks, etc.
    txt = s.astype(str).str.strip()
    txt = txt.str.replace(",", "", regex=False)
    txt = txt.where(~txt.isin(BAD_TOKENS), other=pd.NA)
    return pd.to_numeric(txt, errors="coerce")

def _to_num0(s: pd.Series) -> pd.Series:
    return _to_num_series(s).fillna(0.0)

def _to_num_nan(s: pd.Series) -> pd.Series:
    # keep NaN for grades so we don't turn unknowns into zeros
    return _to_num_series(s)

# ---------- main ----------
for input_model in input_set.get_all("Input Models"):
    source_file_name = _safe_str(input_model.get_attribute("SourceFileName", ""))
    source_directory = _safe_str(input_model.get_attribute("SourceDirectory", "")) or os.getcwd()

    handle.log_info(f"START model: {source_file_name}")
    handle.log_info(f"SourceDirectory (resolved): {source_directory}")

    try:
        df = input_model.read().to_pandas()
        # normalize headers (whitespace issues)
        df.columns = [str(c).strip() for c in df.columns]
        handle.log_info(f"Row count before filter: {len(df)}")
    except Exception as e:
        handle.log_info(f"SKIP: could not read model to pandas. Error: {repr(e)}")
        continue

    missing = REQUIRED_COLS.difference(set(df.columns))
    if missing:
        handle.log_info(f"SKIP: missing required columns: {sorted(list(missing))}")
        continue

    # Filter to crushers
    df_work = df[df["Destination.Top"].astype(str).str.strip() == "Crushers"].copy()
    handle.log_info(f"Row count after Crushers filter: {len(df_work)}")
    if df_work.empty:
        handle.log_info("SKIP: no rows where Destination.Top == 'Crushers'")
        continue

    # Identify tracker cols
    SourceTracker_cols = [c for c in df_work.columns if str(c).startswith("Mining_SourceTracker")]
    handle.log_info(f"Found Mining_SourceTracker cols: {len(SourceTracker_cols)}")
    if not SourceTracker_cols:
        handle.log_info("SKIP: no Mining_SourceTracker* columns found")
        continue

    # --------- convert types ONCE on df_work ----------
    # Text columns
    for c in ["Period.Name","SourceParcel","FinalDestinationFullName","FinalDestination.Top","OriginalSource.OpenPit"]:
        if c in df_work.columns:
            df_work[c] = df_work[c].astype("string")

    # Numeric (fill 0 ok)
    for c in ["Mining_wetTonnes"] + ORETYPE_KEYS + PROD_KEYS + SourceTracker_cols:
        if c in df_work.columns:
            df_work[c] = _to_num0(df_work[c])

    # Grades: keep NaN (DO NOT fill 0 here)
    for c in GRADE_COLS:
        if c in df_work.columns:
            df_work[c] = _to_num_nan(df_work[c])
        else:
            # if truly absent, create NaN col so downstream doesn't default to 0 silently
            df_work[c] = pd.NA

    # optional: log how many grades parsed (non-null)
    try:
        nn = {g: int(df_work[g].notna().sum()) for g in GRADE_COLS}
        handle.log_info(f"Non-null grade counts after parse: {nn}")
    except Exception:
        pass

    # --------- explode by SourceTracker columns ----------
    records = []

    for _, row in df_work.iterrows():
        txn_wet = row.get("Mining_wetTonnes", 0.0)
        if pd.isna(txn_wet) or float(txn_wet) <= 0:
            continue

        period = row.get("Period.Name")
        parcel = row.get("SourceParcel")
        destination_fullname = row.get("FinalDestinationFullName")
        destination_top = row.get("FinalDestination.Top")
        source_openpit = row.get("OriginalSource.OpenPit")

        # numeric values
        oretypes = {k: float(row.get(k, 0.0) or 0.0) for k in ORETYPE_KEYS if k in df_work.columns}
        prods   = {k: float(row.get(k, 0.0) or 0.0) for k in PROD_KEYS if k in df_work.columns}

        # grades can be NaN
        grades = {}
        for g in GRADE_COLS:
            v = row.get(g, pd.NA)
            grades[g] = v  # keep as NaN if unknown

        for col in SourceTracker_cols:
            value = row.get(col, 0.0)
            if pd.isna(value) or float(value) <= 0:
                continue

            parts = str(col).split("_", 2)
            source = parts[2] if len(parts) > 2 else str(col)

            frac = float(value) / float(txn_wet)

            rec = {
                "OriginalSource.OpenPit": source_openpit,
                "FinalDestination.Top": destination_top,
                "OriginalSourceFullName": source,
                "Period.Name": period,
                "SourceParcel": parcel,
                "Mining_wetTonnes": float(value),
                "FinalDestinationFullName": destination_fullname,
            }

            # scaled fields
            for k, v in oretypes.items():
                rec[k] = v * frac
            for k, v in prods.items():
                rec[k] = v * frac

            # grades NOT scaled (as per your original logic)
            for g, v in grades.items():
                rec[g] = v

            records.append(rec)

    report_df = pd.DataFrame(records)
    handle.log_info(f"Expanded records rows: {len(report_df)}")
    if report_df.empty:
        handle.log_info("SKIP: no expanded records after Mining_SourceTracker processing")
        continue

    # --------- aggregation with proper weighted grades ----------
    weight_col = "Mining_wetTonnes"
    group_cols = [
        "Period.Name", "OriginalSourceFullName", "SourceParcel",
        "FinalDestinationFullName", "OriginalSource.OpenPit", "FinalDestination.Top"
    ]

    # Ensure numeric for sums (tonnes/oretypes/prods). Grades keep NaN.
    for c in [weight_col] + ORETYPE_KEYS + PROD_KEYS:
        if c in report_df.columns:
            report_df[c] = pd.to_numeric(report_df[c], errors="coerce").fillna(0.0)

    for g in GRADE_COLS:
        if g in report_df.columns:
            report_df[g] = pd.to_numeric(report_df[g], errors="coerce")  # keep NaN

    # Weighted numerators: only where grade is known
    for g in GRADE_COLS:
        report_df[f"__{g}_num"] = report_df[g] * report_df[weight_col]
        report_df[f"__{g}_w"]   = report_df[weight_col].where(report_df[g].notna(), 0.0)

    sum_cols = [weight_col] + ORETYPE_KEYS + PROD_KEYS + [f"__{g}_num" for g in GRADE_COLS] + [f"__{g}_w" for g in GRADE_COLS]
    sum_cols = [c for c in sum_cols if c in report_df.columns]

    grouped = report_df.groupby(group_cols, as_index=False)[sum_cols].sum()

    # Weighted averages using grade-known weights (prevents all-NaN turning into 0 too early)
    for g in GRADE_COLS:
        num_col = f"__{g}_num"
        w_col   = f"__{g}_w"
        if num_col in grouped.columns and w_col in grouped.columns:
            grouped[g] = grouped[num_col] / grouped[w_col].replace({0: pd.NA})
            grouped[g] = grouped[g].fillna(0.0)  # final fill
            grouped.drop(columns=[num_col, w_col], inplace=True, errors="ignore")
        else:
            grouped[g] = 0.0

    final_report = grouped
    handle.log_info(f"Final report rows: {len(final_report)}")
    
    # 1) Mining_wetTonnes: values < 1 -> 0
    if "Mining_wetTonnes" in final_report.columns:
        final_report.loc[final_report["Mining_wetTonnes"] < 1, "Mining_wetTonnes"] = 0.0

    # 2) Any column containing "oretype" (case-insensitive): values < 1 -> 0
    oretype_cols = [c for c in final_report.columns if "oretype" in str(c).lower()]
    if oretype_cols:
        # Ensure numeric (handles categorical/object/etc.)
        final_report.loc[:, oretype_cols] = final_report.loc[:, oretype_cols].apply(
            lambda s: pd.to_numeric(s.astype(str).str.replace(",", "", regex=False), errors="coerce")
        ).fillna(0.0)

        # Now safe to compare
        final_report.loc[:, oretype_cols] = final_report.loc[:, oretype_cols].mask(
            final_report.loc[:, oretype_cols] < 1, 0.0
        )
    
    # --------- write outputs ----------
    try:
        os.makedirs(source_directory, exist_ok=True)
    except Exception as e:
        handle.log_info(f"ERROR: could not create output dir: {source_directory}. Error: {repr(e)}")
        continue

    base = os.path.splitext(source_file_name)[0] if source_file_name else "input_model"
    output_path = os.path.join(source_directory, f"crusher_report_{base}.csv")
    handle.log_info(f"Writing CSV to: {output_path}")

    try:
        final_report.to_csv(output_path, index=False)
        handle.log_info("DONE: CSV write complete")
    except Exception as e:
        handle.log_info(f"ERROR: failed to write CSV. Error: {repr(e)}")
        continue

    # output model
    output_model = handle.create_model("Table", input_model.label.lower())
    TableModel.from_pandas(final_report).write(output_model.model_path)
    output_set.append_model("Output Models", output_model)
