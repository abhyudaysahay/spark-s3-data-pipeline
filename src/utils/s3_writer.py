from utils.logger import get_logger

logger = get_logger(__name__)

def write_parquet(df, path, mode="overwrite", partition_cols=None):
    logger.info(f"Writing data to {path}")

    writer = df.write.mode(mode)

    if partition_cols:
        writer = writer.partitionBy(*partition_cols)

    writer.parquet(path)

    logger.info(f'data written to {path}')