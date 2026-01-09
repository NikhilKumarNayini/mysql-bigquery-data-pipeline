def load_data(df, output_path: str):
    df.write.mode("overwrite").parquet(output_path)
