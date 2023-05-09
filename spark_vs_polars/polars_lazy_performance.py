import polars as pl
import time

def extraction():
    path1="yellow_tripdata/*.parquet"
    df_trips= pl_read_parquet(path1,)
    path2 = "taxi+_zone_lookup.parquet"
    df_zone = pl_read_parquet(path2,)

    return df_trips, df_zone

def pl_read_parquet(path, ):
    """
    Converting parquet file into Polars dataframe
    """
    df= pl.scan_parquet(path,)
    return df

def transformation(df_trips, df_zone):
    df_trips= mean_test_speed_pl(df_trips, )
    df = df_trips.join(df_zone,how="inner", left_on="PULocationID", right_on="LocationID",)
    df = df.select(["Borough","Zone","trip_distance","passenger_count"])
  
    df = get_Queens_test_speed_pd(df)
    df = round_column(df, "passenger_count",0)
    df = round_column(df, "trip_distance",2)
    
    df = rename_column(df, "passenger_count","mean_passenger_count")
    df = rename_column(df, "trip_distance","mean_trip_distance")
    df = sort_by_columns_desc(df, "mean_trip_distance")
    return df  # return lazy dataframecd 

def rename_column(df, column_old, column_new):
    """
    Renaming columns
    """
    df = df.rename({column_old: column_new})
    return df

def mean_test_speed_pl(df_pl,):
    """
    Getting Mean per PULocationID
    """
    df_pl = df_pl.groupby('PULocationID').agg(pl.col(["trip_distance", "passenger_count"]).mean())
    return df_pl

def sort_by_columns_desc(df, column):
    """
    Sort by column
    """
    df = df.sort(column, descending=True)
    return df

def round_column(df, column,to_round):
    """
    Round numbers on columns
    """
    df = df.with_columns(pl.col(column).round(to_round))
    return df

def get_Queens_test_speed_pd(df_pl):
    """
    Only getting Borough in Queens
    """

    df_pl = df_pl.filter(pl.col("Borough")=='Queens')

    return df_pl

def loading_into_parquet(df_pl):
    """
    Save dataframe in parquet
    """
    df_pl.collect(streaming=True).write_parquet(f'yellow_tripdata_pl.parquet')

def main():
    
    print(f'Starting ETL for Polars')
    start_time = time.perf_counter()

    print('Extracting...')
    df_trips, df_zone =extraction()
       
    end_extract=time.perf_counter() 
    time_extract =end_extract- start_time

    print(f'Extraction Parquet end in {round(time_extract,5)} seconds')
    print('Transforming...')
    df = transformation(df_trips, df_zone)
    end_transform = time.perf_counter() 
    time_transformation =time.perf_counter() - end_extract
    print(f'Transformation end in {round(time_transformation,5)} seconds')
    print('Loading...')
    loading_into_parquet(df,)
    load_transformation =time.perf_counter() - end_transform
    print(f'Loading end in {round(load_transformation,5)} seconds')
    print(f"End ETL for Polars in {str(time.perf_counter()-start_time)}")


if __name__ == "__main__":
    
    main()
