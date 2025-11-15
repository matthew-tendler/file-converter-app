import streamlit as st
import pandas as pd
import pyreadstat
from io import BytesIO
import tempfile
import os
import re

st.set_page_config(page_title="File Converter", layout="centered")

st.title("📁 XPT ↔ Parquet ↔ CSV Converter")
st.write("Upload a file and convert it between XPT, Parquet, and CSV formats.")

def read_xpt(file_bytes):
    """Read XPT bytes into a DataFrame using a temporary file."""
    st.write("DEBUG: read_xpt called")

    # Create a temporary file for XPT input
    with tempfile.NamedTemporaryFile(suffix=".xpt", delete=False) as tmp:
        temp_path = tmp.name
        tmp.write(file_bytes)
    st.write(f"DEBUG: Temp XPT file written to {temp_path}")

    try:
        df, meta = pyreadstat.read_xport(temp_path)
        st.write("DEBUG: XPT file successfully read")
        st.write(f"DEBUG: Rows: {len(df)}, Cols: {len(df.columns)}")
        st.write("DEBUG: XPT metadata", str(meta))
    except Exception as e:
        st.write("DEBUG: Error in read_xpt", str(e))
        raise
    finally:
        try:
            os.remove(temp_path)
            st.write(f"DEBUG: Temp XPT file deleted: {temp_path}")
        except OSError as err:
            st.write(f"DEBUG: Failed to delete temp XPT file: {err}")

    return df

uploaded_file = st.file_uploader("Choose a file", type=["xpt", "parquet", "csv"])

def write_xpt(df):
    st.write("DEBUG: write_xpt called")
    st.write("DEBUG: df head", df.head())
    st.write("DEBUG: dtypes before XPT write", df.dtypes.astype(str))
    st.write("DEBUG: column names", list(df.columns))

    """Write a DataFrame to XPT format and return raw bytes.

    This applies basic SAS XPT constraints:
    - Column names are uppercased, limited to 8 characters, and stripped of
      unsupported characters.
    - Object columns are coerced to strings.
    - Datetime columns are converted to numeric seconds since 1960-01-01.
    """

    # Work on a copy so we do not mutate the original DataFrame
    df_xpt = df.copy()

    # Normalize column names to SAS-style: uppercase, <= 8 chars, safe characters
    safe_cols = []
    seen = set()
    for col in df_xpt.columns:
        # Replace non-alphanumeric/underscore with underscore and uppercase
        base = re.sub(r"[^A-Za-z0-9_]", "_", str(col)).upper()

        # Truncate to 8 characters
        base = base[:8] if len(base) > 8 else base or "COL"

        # Ensure uniqueness by appending numeric suffixes if needed
        new_name = base
        i = 1
        while new_name in seen:
            suffix = str(i)
            new_name = (base[: 8 - len(suffix)] + suffix)
            i += 1

        seen.add(new_name)
        safe_cols.append(new_name)

    df_xpt.columns = safe_cols
    st.write("DEBUG: XPT-safe column names", list(df_xpt.columns))

    # Coerce dtypes to XPT-safe types
    origin = pd.Timestamp("1960-01-01")
    for col in df_xpt.columns:
        series = df_xpt[col]

        # Datetime columns: convert to numeric seconds since 1960-01-01
        if pd.api.types.is_datetime64_any_dtype(series):
            st.write(f"DEBUG: converting datetime column '{col}' to seconds since 1960-01-01")
            df_xpt[col] = (series - origin).dt.total_seconds()

        # Object columns: coerce to string
        elif series.dtype == "object":
            st.write(f"DEBUG: converting object column '{col}' to string")
            df_xpt[col] = series.astype(str)

    # Create a temporary file path
    with tempfile.NamedTemporaryFile(suffix=".xpt", delete=False) as tmp:
        temp_path = tmp.name

    # Write XPT to that path
    pyreadstat.write_xport(df_xpt, temp_path)

    # Read the bytes back into memory
    try:
        with open(temp_path, "rb") as f:
            data = f.read()
    finally:
        # Always try to clean up the temp file
        try:
            os.remove(temp_path)
        except OSError:
            pass

    return data

def read_parquet(file_bytes):
    return pd.read_parquet(BytesIO(file_bytes))

def write_parquet(df):
    st.write("DEBUG: write_parquet called")
    st.write("DEBUG: df head", df.head())
    import pyarrow as pa
    import pyarrow.parquet as pq

    # Preserve true nulls in string columns instead of converting to empty strings.
    buffer = BytesIO()
    table = pa.Table.from_pandas(
        df,
        preserve_index=False,
        strings_can_be_null=True,
    )
    pq.write_table(table, buffer, version="2.6")
    return buffer.getvalue()

def read_csv(file_bytes):
    """Read CSV bytes into a DataFrame, preserving values as strings.

    Using dtype=str and keep_default_na=False avoids pandas guessing types
    or turning empty strings into NaN, which is important for clinical data.
    """
    return pd.read_csv(
        BytesIO(file_bytes),
        dtype=str,
        keep_default_na=False
    )

def write_csv(df):
    buffer = BytesIO()
    df.to_csv(buffer, index=False)
    return buffer.getvalue()

if uploaded_file:
    file_bytes = uploaded_file.read()
    filename = uploaded_file.name
    ext = filename.split(".")[-1].lower()

    st.write(f"Detected file type: **{ext.upper()}**")

    # Map input extension to reader function
    readers = {
        "xpt": read_xpt,
        "parquet": read_parquet,
        "csv": read_csv,
    }

    writers = {
        "xpt": write_xpt,
        "parquet": write_parquet,
        "csv": write_csv,
    }

    # Allowed conversion targets
    allowed_targets = {
        "xpt": ["parquet", "csv"],
        "parquet": ["xpt", "csv"],
        "csv": ["xpt", "parquet"],
    }

    df = readers[ext](file_bytes)

    tabs = st.tabs(["Converter", "Smoke Tester"])
    with tabs[0]:
        st.subheader("Select output format")
        target_format = st.selectbox(
            "Convert to:", 
            allowed_targets.get(ext, []),
            format_func=lambda x: x.upper()
        )

        if st.button("Convert"):
            if not target_format:
                st.error("Please select an output format before converting.")
                st.stop()
            try:
                st.write("DEBUG: DataFrame loaded", {"rows": len(df), "cols": len(df.columns)})
                st.write("DEBUG: dtypes", df.dtypes.astype(str))
                st.write(f"DEBUG: Calling writer for {target_format}")
                output_bytes = writers[target_format](df)

                # create output filename
                out_name = re.sub(r"(?i)\." + ext + "$", f".{target_format}", filename)

                st.success("Conversion complete!")
                st.download_button(
                    f"Download {target_format.upper()}",
                    data=output_bytes,
                    file_name=out_name,
                )
            except Exception as e:
                import traceback
                st.error(f"Conversion failed: {e}")
                st.code(traceback.format_exc())
    with tabs[1]:
        st.subheader("Smoke Tester Diagnostics")
        st.write("Preview of loaded data:")
        st.dataframe(df.head())

        st.write("Column dtypes:")
        st.write(df.dtypes.astype(str))

        st.write("Missing values per column:")
        st.write(df.isna().sum())

        st.write("Unique counts (first 20 columns):")
        st.write({col: df[col].nunique() for col in df.columns[:20]})

        st.subheader("Round-trip Fidelity Checks")

        formats_to_test = ["parquet", "csv", "xpt"]
        for fmt in formats_to_test:
            st.write(f"### Testing round-trip for {fmt.upper()}")
            try:
                written_bytes = writers[fmt](df)
                st.write(f"Write {fmt} successful.")

                read_back = readers[fmt](written_bytes)
                st.write(f"Read-back rows: {len(read_back)}, cols: {len(read_back.columns)}")

                # Row count consistency
                st.write("Row count difference:", len(read_back) - len(df))

                # Dtype comparison
                dtype_before = df.dtypes.astype(str).to_dict()
                dtype_after = read_back.dtypes.astype(str).to_dict()
                st.write("Dtype mismatches:", {k: (dtype_before.get(k), dtype_after.get(k)) 
                                               for k in dtype_before if dtype_before.get(k) != dtype_after.get(k)})

                # ---------- Advanced Smoke Tests ----------
                st.write("Advanced Checks:")

                # Check for columns present before but missing after round‑trip
                missing_after = [col for col in df.columns if col not in read_back.columns]
                st.write("Columns missing after conversion:", missing_after)

                # Check for columns that appeared unexpectedly
                extra_after = [col for col in read_back.columns if col not in df.columns]
                st.write("Unexpected new columns after conversion:", extra_after)

                # Check if any column changed dtype category (string->numeric, numeric->string, etc.)
                dtype_category_mismatch = {
                    col: (str(df[col].dtype), str(read_back[col].dtype))
                    for col in df.columns
                    if col in read_back.columns
                    and pd.api.types.infer_dtype(df[col], skipna=True) != pd.api.types.infer_dtype(read_back[col], skipna=True)
                }
                st.write("Dtype category mismatches:", dtype_category_mismatch)

                # ---------- Value-Level Diffing (sample-based) ----------
                st.write("Value-Level Diff Samples (first 10 mismatches per column):")

                value_diffs = {}
                common_cols = [c for c in df.columns if c in read_back.columns]

                for col in common_cols:
                    try:
                        original_series = df[col].astype(str).fillna("<NA>")
                        new_series = read_back[col].astype(str).fillna("<NA>")

                        mismatches = original_series.ne(new_series)
                        if mismatches.any():
                            mismatch_indices = mismatches[mismatches].index[:10]
                            value_diffs[col] = pd.DataFrame({
                                "index": mismatch_indices,
                                "original": original_series.loc[mismatch_indices].values,
                                "converted": new_series.loc[mismatch_indices].values,
                            })
                    except Exception as e:
                        value_diffs[col] = f"Error comparing column: {e}"

                if value_diffs:
                    st.write(value_diffs)
                else:
                    st.write("No value-level mismatches detected in sampled rows.")
            except Exception as e:
                st.error(f"Round-trip failed for {fmt}: {e}")