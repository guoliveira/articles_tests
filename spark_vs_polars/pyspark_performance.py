from pyspark.sql import SparkSession
from pyspark.sql.functions import mean, col, round
import time

def round_column(df, column,to_round):
    """
    Round numbers on columns
    """
    df = df.withColumn(column, round(col(column), to_round))

    return df

def get_Queens_test_speed_py(df):
    """
    Only getting Borough in Queens
    """
    df = df.filter(col("Borough") == "Queens")
    return df

def mean_test_speed_py(df,):
    """
    Getting Mean per PULocationID
    """
    df = df.groupby("PULocationID").agg(mean("trip_distance").alias("mean_trip_distance"), mean("passenger_count").alias("mean_passenger_count"))

    return df


def extraction():
    spark = SparkSession.builder.appName("ETL").getOrCreate()
    path1 = "yellow_tripdata"
    df_trips = spark.read.parquet(path1)
    path2 = "taxi+_zone_lookup.parquet"
    df_zone = spark.read.parquet(path2)
    return df_trips, df_zone

def transformation(df_trips, df_zone):
    # Get Mean
    df_trips=mean_test_speed_py(df_trips)

    df = df_trips.join(df_zone, col("PULocationID") == col("LocationID"), "inner")
    df = df.select(col("Borough"), col("Zone"), col("mean_trip_distance"), col("mean_passenger_count"))
    
    # Get one Borough
    df= get_Queens_test_speed_py(df)
    # Round columns
    df = round_column(df,"mean_passenger_count", 0)
    df = round_column(df,"mean_trip_distance", 2)

    df = df.orderBy(col("mean_trip_distance").desc())
    return df

def loading_into_parquet(df):
    df.write.parquet("yellow_tripdata_pyspark", mode="overwrite")

def main():
    print("Starting ETL for PySpark")
    start_time = time.perf_counter()

    print("Extracting...")
    df_trips, df_zone = extraction()

    end_extract = time.perf_counter()
    time_extract = end_extract - start_time
    print(f"Extraction Parquet end in {str(time_extract)} seconds")
    print("Transforming...")
    df = transformation(df_trips, df_zone)
    end_transform = time.perf_counter()
    time_transformation = end_transform - end_extract
    print(f"Transformation end in {str(time_transformation)} seconds")
    print("Loading...")
    loading_into_parquet(df)
    load_transformation = time.perf_counter() - end_transform
    print(f"Loading end in {str(load_transformation)} seconds")
    print(f"End ETL for PySpark in {str(time.perf_counter()-start_time)}")


if __name__ == "__main__":
    
    main()