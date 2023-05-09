import requests
import time
import polars as pl

def main():
    new_df = pl.scan_parquet("yellow_tripdata/*.parquet")
    # print(new_df.select(pl.count()))
    new_df = new_df.select(pl.col(['VendorID', 'tpep_pickup_datetime', 'tpep_dropoff_datetime'
                                   ,'PULocationID', 'DOLocationID','payment_type'
                                   , 'passenger_count' , 'trip_distance']))

    print(new_df.collect())
    #for j in range(18,19):
    #    for i in range(1,13):
    #        file = f"yellow_tripdata_20{str(j).zfill(2)}-{str(i).zfill(2)}.parquet"
    #        print(file)
    #        df2 = pl.scan_parquet(file,)
    #        df2 = df2.select(pl.col(['VendorID', 'tpep_pickup_datetime', 'tpep_dropoff_datetime'
    #                               ,'PULocationID', 'DOLocationID','payment_type'
    #                               , 'passenger_count' , 'trip_distance']))
    #        new_df = pl.concat([new_df, df2], rechunk=True)

    #new_df.collect().write_parquet("yellow_tripdata1822.parquet")
            #file =f"https://github.com/DataTalksClub/nyc-tlc-data/releases/download/yellow/yellow_tripdata_20{str(j).zfill(2)}-{str(i).zfill(2)}.csv.gz"
            #print(file)
           # with requests.get(file) as response:
           #     open(f"yellow_tripdata_20{str(j).zfill(2)}-{str(i).zfill(2)}.csv", "wb").write(response.content)




if __name__ == "__main__":
    
    main()