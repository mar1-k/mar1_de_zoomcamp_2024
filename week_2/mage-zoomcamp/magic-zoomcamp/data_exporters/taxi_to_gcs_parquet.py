from mage_ai.settings.repo import get_repo_path
from mage_ai.io.config import ConfigFileLoader
from mage_ai.io.google_cloud_storage import GoogleCloudStorage
from pandas import DataFrame
import pyarrow as pa
import pyarrow.parquet as pq
import os

if 'data_exporter' not in globals():
    from mage_ai.data_preparation.decorators import data_exporter


os.environ['GOOGLE_APPLICATION_CREDENTIALS'] = "OMITTED.json"

project_id = 'de-zoomcamp-412210'
bucket_name = 'zoomcamp-week2-mage'
object_key = 'nyc_taxi_data.parquet'
table_name = 'nyc_taxi_data_take_2'
root_path = f'{bucket_name}/{table_name}'

@data_exporter
def export_data_to_google_cloud_storage(df: DataFrame, **kwargs) -> None:
    df['lpep_pickup_date'] =  df['lpep_pickup_datetime'].dt.date
    table = pa.Table.from_pandas(df)
    gcs = pa.fs.GcsFileSystem()

    pq.write_to_dataset(
        table,
        root_path = root_path,
        partition_cols=['lpep_pickup_date'],
        filesystem= gcs
    )
