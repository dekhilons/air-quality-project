import glob
from pyspark.sql import SparkSession, functions as F
from pyspark.sql.types import StructType, StructField, IntegerType, StringType, DoubleType

RAW_PATH = "data/raw/openaq/*/*/*/*.csv"
OUT_PATH = "data/processed/openaq_raw"

NUM_YEARS = 9

openaq_schema = StructType([
    StructField("location_id", IntegerType()),
    StructField("sensors_id", IntegerType()),
    StructField("location", StringType()),
    StructField("datetime", StringType()), 
    StructField("lat", DoubleType()),
    StructField("lon", DoubleType()),
    StructField("parameter", StringType()),
    StructField("units", StringType()),
    StructField("value", DoubleType()),
    StructField("_corrupt_record", StringType()),
])


def main():

    spark = SparkSession.builder.appName("ingest_openaq").getOrCreate()

    files = glob.glob(RAW_PATH)
    print(f"Found {len(files)} OpenAQ CSV files")

    df = (
        spark.read
        .schema(openaq_schema)
        .option("header", True)
        .option("mode", "PERMISSIVE")
        .option("columnNameOfCorruptRecord", "_corrupt_record")
        .csv(files)
    )

    df = df.withColumn("year", F.substring("datetime", 1, 4).cast("int"))

    df.printSchema()
    df.show(5)

    (
        df.repartition(NUM_YEARS, "year")
        .write.mode("overwrite")
        .partitionBy("year")
        .parquet(OUT_PATH)
    )
    print(f"Saved to {OUT_PATH}")


    saved = spark.read.parquet(OUT_PATH)

    stats = saved.agg(
        F.count(F.lit(1)).alias("total_rows"),
        F.count("_corrupt_record").alias("corrupt_rows"),
        F.sum(F.col("location_id").isNull().cast("int")).alias("null_location_id"),
        F.sum(F.col("datetime").isNull().cast("int")).alias("null_datetime"),
        F.sum(F.col("value").isNull().cast("int")).alias("null_value"),
    ).first()

    distinct_key_rows = saved.dropDuplicates(["location_id", "sensors_id", "datetime"]).count()

    print("Total rows:", stats["total_rows"])
    print("Corrupt rows (didn't match the schema):", stats["corrupt_rows"])
    print("Null location_id:", stats["null_location_id"])
    print("Null datetime:", stats["null_datetime"])
    print("Null value:", stats["null_value"])
    print("Duplicate rows (same location_id + sensors_id + datetime):", stats["total_rows"] - distinct_key_rows)

    spark.stop()


if __name__ == "__main__":
    main()
