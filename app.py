import streamlit as st
import pandas as pd
import pyreadstat
import pyarrow as pa
import pyarrow.parquet as pq
import tempfile
import os
from io import BytesIO
import re

st.set_page_config(page_title="Clinical File Converter", layout="wide")

# ------------------------------------------------------------
# Readers
# ------------------------------------------------------------
def read_csv(file_bytes):
    return pd.read_csv(
        BytesIO(file_bytes),
        dtype=str,
        keep_default_na=False
    )

def read_parquet(file_bytes):
    table = pq.read_table(BytesIO(file_bytes))
    return table.to_pandas()

def read_xpt(file_bytes):
    with tempfile.NamedTemporaryFile(suffix=".xpt", delete=False) as tmp:
        tmp.write(file_bytes)
        tmp_path = tmp.name
    try:
        df, meta = pyreadstat.read_xport(tmp_path)
    finally:
        try:
            os.remove(tmp_path)
        except:
            pass
    return df

readers = {
    "csv": read_csv,
    "parquet": read_parquet,
    "xpt": read_xpt,
}

# ------------------------------------------------------------
# Writers
# ------------------------------------------------------------
def write_csv(df):
    buf = BytesIO()
    df.to_csv(buf, index=False)
    return buf.getvalue()

def write_parquet(df):
    st.write("DEBUG: write_parquet called")

    buffer = BytesIO()

    arrays = []
    fields = []

    for col in df.columns:
        series = df[col]
        arr = pa.array(series, from_pandas=True)
        arrays.append(arr)
        fields.append(pa.field(col, arr.type))

    schema = pa.schema(fields)
    table = pa.Table.from_arrays(arrays, schema=schema)

    pq.write_table(table, buffer, version="2.6")
    return buffer.getvalue()


def write_xpt(df):
    df_xpt = df.copy()

    safe_cols = []
    seen = set()

    for col in df_xpt.columns:
        base = re.sub(r"[^A-Za-z0-9_]", "_", str(col)).upper()
        base = base[:8] if len(base) > 8 else base or "COL"

        new = base
        i = 1
        while new in seen:
            suffix = str(i)
            new = base[: 8 - len(suffix)] + suffix
            i += 1

        seen.add(new)
        safe_cols.append(new)

    df_xpt.columns = safe_cols

    origin = pd.Timestamp("1960-01-01")
    for col in df_xpt.columns:
        series = df_xpt[col]
        if pd.api.types.is_datetime64_any_dtype(series):
            df_xpt[col] = (series - origin).dt.total_seconds()
        elif series.dtype == "object":
            df_xpt[col] = series.astype(str)

    with tempfile.NamedTemporaryFile(suffix=".xpt", delete=False) as tmp:
        tmp_path = tmp.name

    pyreadstat.write_xport(df_xpt, tmp_path)

    try:
        with open(tmp_path, "rb") as f:
            data = f.read()
    finally:
        try:
            os.remove(tmp_path)
        except:
            pass

    return data

writers = {
    "csv": write_csv,
    "parquet": write_parquet,
    "xpt": write_xpt,
}

# ------------------------------------------------------------
# UI
# ------------------------------------------------------------
st.title("Clinical File Converter")

uploaded = st.file_uploader("Upload CSV, Parquet, or XPT", type=["csv", "parquet", "xpt"])

if uploaded:
    fmt = uploaded.name.split(".")[-1].lower()
    if fmt not in readers:
        st.error("Unsupported file type")
    else:
        df = readers[fmt](uploaded.read())
        st.success(f"Loaded file with {len(df)} rows and {len(df.columns)} columns.")

        tabs = st.tabs(["Converter", "Smoke Tester"])

        # ------------------------------------------------------------
        # Converter Tab
        # ------------------------------------------------------------
        with tabs[0]:
            st.subheader("Convert File")

            target_fmt = st.selectbox("Convert to format", ["csv", "parquet", "xpt"])
            if st.button("Convert"):
                out_bytes = writers[target_fmt](df)
                st.download_button(
                    f"Download {target_fmt.upper()}",
                    out_bytes,
                    file_name=f"converted.{target_fmt}",
                )

        # ------------------------------------------------------------
        # Smoke Tester Tab
        # ------------------------------------------------------------
        with tabs[1]:
            st.subheader("Smoke Tester Diagnostics")

            st.write("Preview of loaded data:")
            st.dataframe(df.head())

            st.write("Column dtypes:")
            st.write(df.dtypes.astype(str))

            st.write("Missing values:")
            st.write(df.isna().sum())

            st.write("Unique (first 20 columns):")
            st.write({col: df[col].nunique() for col in df.columns[:20]})

            st.subheader("Round‑trip Fidelity Checks")

            for fmt in ["csv", "parquet", "xpt"]:
                st.write(f"### Testing {fmt.upper()}")

                try:
                    written = writers[fmt](df)
                    read_back = readers[fmt](written)

                    st.write("Rows before:", len(df), "after:", len(read_back))

                    dtype_before = df.dtypes.astype(str).to_dict()
                    dtype_after = read_back.dtypes.astype(str).to_dict()

                    mismatches = {
                        c: (dtype_before.get(c), dtype_after.get(c))
                        for c in dtype_before
                        if c in dtype_after and dtype_before[c] != dtype_after[c]
                    }

                    st.write("Dtype mismatches:", mismatches)

                    # Value-level diffing
                    diffs = {}
                    common = [c for c in df.columns if c in read_back.columns]

                    for col in common:
                        orig = df[col].astype(str).fillna("<NA>")
                        new = read_back[col].astype(str).fillna("<NA>")

                        mismatch_mask = orig != new
                        if mismatch_mask.any():
                            idx = mismatch_mask[mismatch_mask].index[:10]
                            diffs[col] = pd.DataFrame({
                                "index": idx,
                                "original": orig.loc[idx],
                                "converted": new.loc[idx],
                            })

                    if diffs:
                        st.write("Value diffs:", diffs)
                    else:
                        st.write("No value mismatches detected.")

                except Exception as e:
                    st.error(f"Failed round‑trip for {fmt}: {e}")