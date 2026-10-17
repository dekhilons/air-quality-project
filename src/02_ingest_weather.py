import glob
from pyspark.sql import SparkSession, functions as F
from pyspark.sql.types import StructType, StructField, IntegerType, StringType, DoubleType


RAW_PATH = "data/raw/weather/*/*/*.csv"
OUT_PATH = "data/processed/weather_raw"

NUM_OUTPUT_FILES = 8


weather_schema = StructType([
    StructField("location_id", IntegerType()),
    StructField("station", StringType()),
    StructField("station_latitude", DoubleType()),
    StructField("station_longitude", DoubleType()),
    StructField("datetime_utc", StringType()), 
    StructField("temperature_2m", DoubleType()),
    StructField("relative_humidity_2m", DoubleType()),
    StructField("precipitation", DoubleType()),
    StructField("wind_speed_10m", DoubleType()),
    StructField("_corrupt_record", StringType()),
])


def main():
    spark = SparkSession.builder.appName("ingest_weather").getOrCreate()

    files = glob.glob(RAW_PATH)
    print(f"Found {len(files)} weather CSV files")

    df = (
        spark.read
        .schema(weather_schema)
        .option("header", True)
        .option("mode", "PERMISSIVE")
        .option("columnNameOfCorruptRecord", "_corrupt_record")
        .csv(files)
    )

    df.printSchema()
    df.show(5)

    df.coalesce(NUM_OUTPUT_FILES).write.mode("overwrite").parquet(OUT_PATH)
    print(f"Saved to {OUT_PATH}")


    saved = spark.read.parquet(OUT_PATH)

    stats = saved.agg(
        F.count(F.lit(1)).alias("total_rows"),
        F.count("_corrupt_record").alias("corrupt_rows"),
        F.sum(F.col("location_id").isNull().cast("int")).alias("null_location_id"),
        F.sum(F.col("datetime_utc").isNull().cast("int")).alias("null_datetime_utc"),
        F.sum(F.col("temperature_2m").isNull().cast("int")).alias("null_temperature_2m"),
    ).first()

    distinct_key_rows = saved.dropDuplicates(["location_id", "datetime_utc"]).count()

    print("Total rows:", stats["total_rows"])
    print("Corrupt rows (didn't match the schema):", stats["corrupt_rows"])
    print("Null location_id:", stats["null_location_id"])
    print("Null datetime_utc:", stats["null_datetime_utc"])
    print("Null temperature_2m:", stats["null_temperature_2m"])
    print("Duplicate rows (same location_id + datetime_utc):", stats["total_rows"] - distinct_key_rows)

    spark.stop()


if __name__ == "__main__":
    main()
