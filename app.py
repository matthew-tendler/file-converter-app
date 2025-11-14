import streamlit as st
import pandas as pd
import pyreadstat
from io import BytesIO
import pyarrow

st.set_page_config(page_title="File Converter", layout="centered")

st.title("📁 XPT ↔ Parquet ↔ CSV Converter")
st.write("Upload a file and convert it between XPT, Parquet, and CSV formats.")

uploaded_file = st.file_uploader("Choose a file", type=["xpt", "parquet", "csv"])

def read_xpt(file_bytes):
    df, _ = pyreadstat.read_xport(BytesIO(file_bytes))
    return df

def write_xpt(df):
    buffer = BytesIO()
    pyreadstat.write_xport(df, buffer)
    return buffer.getvalue()

def read_parquet(file_bytes):
    return pd.read_parquet(BytesIO(file_bytes))

def write_parquet(df):
    buffer = BytesIO()
    df.to_parquet(buffer, index=False)
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
        try:
            df = readers[ext](file_bytes)
            output_bytes = writers[target_format](df)

            # create output filename
            out_name = filename.replace(f".{ext}", f".{target_format}")

            st.success("Conversion complete!")
            st.download_button(
                f"Download {target_format.upper()}",
                data=output_bytes,
                file_name=out_name,
            )
        except Exception as e:
            st.error(f"Conversion failed: {e}")