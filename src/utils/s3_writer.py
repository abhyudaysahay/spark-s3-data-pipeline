def write_parquet(df, path, mode="overwrite", partition_cols=None):
    print(f"Writing data to {path}")

    writer = df.write.mode(mode)

    if partition_cols:
        writer = writer.partitionBy(*partition_cols)

    writer.parquet(path)

    print(f'data written to {path}')