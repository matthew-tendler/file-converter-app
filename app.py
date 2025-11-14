import streamlit as st
import pandas as pd
import pyreadstat
from io import BytesIO
import pyarrow

st.set_page_config(page_title="File Converter", layout="centered")

st.title("📁 XPT ↔ Parquet ↔ CSV Converter")
st.write("Upload a file and convert it between XPT, Parquet, and CSV formats.")

uploaded_file = st.file_uploader("Choose a file", type=["xpt", "parquet", "csv"])

def convert_xpt_to_parquet(file_bytes):
    df, meta = pyreadstat.read_xport(BytesIO(file_bytes))
    buffer = BytesIO()
    df.to_parquet(buffer, index=False)
    return buffer.getvalue()

def convert_parquet_to_csv(file_bytes):
    df = pd.read_parquet(BytesIO(file_bytes))
    buffer = BytesIO()
    df.to_csv(buffer, index=False)
    return buffer.getvalue()

def convert_csv_to_parquet(file_bytes):
    df = pd.read_csv(BytesIO(file_bytes))
    buffer = BytesIO()
    df.to_parquet(buffer, index=False)
    return buffer.getvalue()

if uploaded_file:
    file_bytes = uploaded_file.read()
    filename = uploaded_file.name
    ext = filename.split(".")[-1].lower()

    st.write(f"Detected file type: **{ext.upper()}**")

    if ext == "xpt":
        st.subheader("Convert XPT → Parquet")
        if st.button("Convert"):
            output = convert_xpt_to_parquet(file_bytes)
            st.success("Conversion complete!")
            st.download_button(
                "Download Parquet",
                data=output,
                file_name=filename.replace(".xpt", ".parquet"),
            )

    elif ext == "parquet":
        st.subheader("Convert Parquet → CSV")
        if st.button("Convert"):
            output = convert_parquet_to_csv(file_bytes)
            st.success("Conversion complete!")
            st.download_button(
                "Download CSV",
                data=output,
                file_name=filename.replace(".parquet", ".csv"),
            )

    elif ext == "csv":
        st.subheader("Convert CSV → Parquet")
        if st.button("Convert"):
            output = convert_csv_to_parquet(file_bytes)
            st.success("Conversion complete!")
            st.download_button(
                "Download Parquet",
                data=output,
                file_name=filename.replace(".csv", ".parquet"),
            )