import pandas as pd
from google.cloud import bigquery
from loggers.logger import get_logger


class BigQueryWriter:
    def __init__(self, writer_config: dict):
        self.writer_config = writer_config
        self.project_id = writer_config["PROJECT_ID"]
        self.target_table = writer_config["TARGET_TABLE"]
        self.staging_table = writer_config["STAGING_TABLE"]
        self.unique_key = writer_config["UNIQUE_KEY"]
        self.client = bigquery.Client(project=self.project_id)
        self.logger = get_logger(self.__class__.__name__)
    
    def infer_schema(self, df: pd.DataFrame):
        schema = []
        for col in df.columns:
            if col == "raw_json":
                schema.append(bigquery.SchemaField(col, "JSON"))
            elif pd.api.types.is_integer_dtype(df[col]):
                schema.append(bigquery.SchemaField(col, "INT64"))
            elif pd.api.types.is_float_dtype(df[col]):
                schema.append(bigquery.SchemaField(col, "FLOAT64"))
            elif pd.api.types.is_bool_dtype(df[col]):
                schema.append(bigquery.SchemaField(col, "BOOL"))
            else:
                schema.append(bigquery.SchemaField(col, "STRING"))
        
        return schema

    def write_staging(self, df: pd.DataFrame):
        job_config = bigquery.LoadJobConfig(
            schema=self.infer_schema(df),
            write_disposition="WRITE_TRUNCATE"
        )
        records = df.to_dict(orient="records")
        load_job = self.client.load_table_from_json(
            records,
            self.staging_table,
            job_config=job_config
        )
        load_job.result()
        self.logger.info(f"SUCCESS: Wrote {len(records)} rows to staging table {self.staging_table}")

    def merge_upsert(self):
        merge_sql = f"""
        MERGE `{self.target_table}` AS target
        USING `{self.staging_table}` AS source
        ON target.{self.unique_key} = source.{self.unique_key}
        WHEN MATCHED THEN
          UPDATE SET {', '.join([f"target.{c}=source.{c}" for c in self.get_columns()])}
        WHEN NOT MATCHED THEN
          INSERT ({', '.join(self.get_columns())})
          VALUES ({', '.join([f"source.{c}" for c in self.get_columns()])});
        """
        query_job = self.client.query(merge_sql)
        query_job.result()
        self.logger.info(f"SUCCESS: Wrote successfuly to BigQuert target table: {self.target_table}")

    def get_columns(self):
        table = self.client.get_table(self.target_table)
        return [field.name for field in table.schema]

    def write(self, df: pd.DataFrame):
        if df.empty:
            self.logger.info("DataFrame is empty, skipping BigQuery write")
            return
        self.write_staging(df)
        self.merge_upsert()
