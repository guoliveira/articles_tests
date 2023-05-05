import requests
import time
import polars as pl

def pl_read_csv(path, ):
    """
    Converting parquet file into Pandas dataframe
    """
    df= pl.read_csv(path,infer_schema_length=10000)
    return df

def main():
    new_df = pl.read_parquet("yellow_tripdata.parquet")
    new_df = new_df.select(pl.col(['VendorID', 'tpep_pickup_datetime', 'tpep_dropoff_datetime'
                                   ,'PULocationID', 'DOLocationID','payment_type'
                                   , 'passenger_count' , 'trip_distance']))


    for j in range(21,23):
        for i in range(1,13):
            file = f"yellow_tripdata_20{str(j).zfill(2)}-{str(i).zfill(2)}.parquet"
            print(file)
            df2 = pl.read_parquet(file,)
            df2 = df2.select(pl.col(['VendorID', 'tpep_pickup_datetime', 'tpep_dropoff_datetime'
                                   ,'PULocationID', 'DOLocationID','payment_type'
                                   , 'passenger_count' , 'trip_distance']))
            new_df = new_df.vstack(df2)

    new_df.write_parquet("yellow_tripdata1922.parquet")
            #file =f"https://github.com/DataTalksClub/nyc-tlc-data/releases/download/yellow/yellow_tripdata_20{str(j).zfill(2)}-{str(i).zfill(2)}.csv.gz"
            #print(file)
           # with requests.get(file) as response:
           #     open(f"yellow_tripdata_20{str(j).zfill(2)}-{str(i).zfill(2)}.csv", "wb").write(response.content)




if __name__ == "__main__":
    
    main()