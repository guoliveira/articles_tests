import pandas as pd
import os
import datetime

path="yellow_tripdata_pl.parquet"
df = pd.read_parquet(path)
print(f'Output of Polars: {path} modify at {datetime.datetime.fromtimestamp(os.path.getmtime(path))}')
print(df)


py_path= "yellow_tripdata_pyspark"
df_py = pd.read_parquet(py_path)

print(f'Output of Pyspark: {py_path} modify at {datetime.datetime.fromtimestamp(os.path.getmtime(py_path))}')
print(df_py)