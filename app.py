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

    pyreadstat.write_xport expects a filesystem path, not a file-like object,
    so we write to a temporary file and then read the contents back.
    """
    # Create a temporary file path
    with tempfile.NamedTemporaryFile(suffix=".xpt", delete=False) as tmp:
        temp_path = tmp.name

    # Write XPT to that path
    pyreadstat.write_xport(df, temp_path)

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
    buffer = BytesIO()
    table = pa.Table.from_pandas(df, preserve_index=False)
    pq.write_table(table, buffer, version="2.6")
    return buffer.getvalue()

def read_csv(file_bytes):
    return pd.read_csv(BytesIO(file_bytes))

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
            df = readers[ext](file_bytes)
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