# file-converter-app
# File Converter App

A simple Streamlit-based web application that converts files between **XPT**, **Parquet**, and **CSV** formats.

## Features

- Auto-detects uploaded file type  
- Converts between:
  - XPT → Parquet / CSV
  - Parquet → CSV / XPT
  - CSV → Parquet / XPT
- Download-ready output files  
- Runs locally or on Streamlit Cloud  

## Usage

1. Open the app in your browser.
2. Upload a file (`.xpt`, `.parquet`, `.csv`).
3. Select the desired output format.
4. Click **Convert**.
5. Download the converted file.

## Local Development

```bash
pip install -r requirements.txt
streamlit run app.py
```

## Supported Formats

- **XPT** (SAS Transport)
- **Parquet**
- **CSV**

## License

MIT