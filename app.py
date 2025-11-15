def write_parquet(df):
    st.write("DEBUG: write_parquet called")
    st.write("DEBUG: df head", df.head())
    import pyarrow as pa
    import pyarrow.parquet as pq

    buffer = BytesIO()

    # Build Arrow arrays column by column to preserve true nulls.
    arrays = []
    fields = []

    for col in df.columns:
        series = df[col]
        arr = pa.array(series, from_pandas=True)  # respects pandas NA mask
        arrays.append(arr)
        fields.append(pa.field(col, arr.type))

    schema = pa.schema(fields)
    table = pa.Table.from_arrays(arrays, schema=schema)

    pq.write_table(table, buffer, version="2.6")

    return buffer.getvalue()