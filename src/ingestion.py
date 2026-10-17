import os
import time

from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import StructType, StructField, IntegerType, StringType, DoubleType

start_time = time.time()

spark = SparkSession.builder \
    .appName("AirQualityIngestion") \
    .master("local[*]") \
    .config("spark.driver.memory", "4g") \
    .config("spark.sql.session.timeZone", "UTC") \
    .config("spark.sql.files.openCostInBytes", "65536") \
    .config("spark.ui.showConsoleProgress", "false") \
    .getOrCreate()
spark.sparkContext.setLogLevel("ERROR")

OPENAQ_RAW = "data/raw/openaq"
WEATHER_RAW = "data/raw/weather"
STATIONS_RAW = "config/openaq_station_candidates.csv"
OPENAQ_OUT = "data/processed/openaq"
WEATHER_OUT = "data/processed/weather"
STATIONS_OUT = "data/processed/stations"
DATETIME_FORMAT = "yyyy-MM-dd'T'HH:mm:ssXXX"


def size_mb(path):
    if os.path.isfile(path):
        return round(os.path.getsize(path) / 1e6, 2)
    total = 0
    for root, dirs, files in os.walk(path):
        for name in files:
            total += os.path.getsize(os.path.join(root, name))
    return round(total / 1e6, 2)


def summary(name, df):
    print("=" * 70)
    print(name)
    print("=" * 70)
    df.printSchema()
    print("Rows:", df.count())
    df.show(5, truncate=False)
    df.agg(
        F.sum(F.when(F.col("datetime_utc").isNull(), 1).otherwise(0)).alias("null_datetime_utc"),
        F.min("datetime_utc").alias("min_datetime_utc"),
        F.max("datetime_utc").alias("max_datetime_utc"),
        F.countDistinct("location_id").alias("distinct_stations")
    ).show(truncate=False)
    df.groupBy("year").count().orderBy("year").show()


openaq_schema = StructType([
    StructField("location_id", IntegerType(), True),
    StructField("sensors_id", IntegerType(), True),
    StructField("location", StringType(), True),
    StructField("datetime", StringType(), True),
    StructField("lat", DoubleType(), True),
    StructField("lon", DoubleType(), True),
    StructField("parameter", StringType(), True),
    StructField("units", StringType(), True),
    StructField("value", DoubleType(), True)
])

weather_schema = StructType([
    StructField("location_id", IntegerType(), True),
    StructField("station", StringType(), True),
    StructField("station_latitude", DoubleType(), True),
    StructField("station_longitude", DoubleType(), True),
    StructField("datetime_utc", StringType(), True),
    StructField("temperature_2m", DoubleType(), True),
    StructField("relative_humidity_2m", DoubleType(), True),
    StructField("precipitation", DoubleType(), True),
    StructField("wind_speed_10m", DoubleType(), True)
])

stations_schema = StructType([
    StructField("location_id", IntegerType(), True),
    StructField("station", StringType(), True),
    StructField("locality", StringType(), True),
    StructField("latitude", DoubleType(), True),
    StructField("longitude", DoubleType(), True),
    StructField("pollutants", StringType(), True),
    StructField("sensor_count", IntegerType(), True),
    StructField("pollutant_set", StringType(), True),
    StructField("target_count", IntegerType(), True)
])

openaq_raw = spark.read \
    .schema(openaq_schema) \
    .option("header", True) \
    .option("recursiveFileLookup", True) \
    .option("pathGlobFilter", "*.csv") \
    .csv(OPENAQ_RAW)
print("OpenAQ CSV files read:", len(openaq_raw.inputFiles()))

check_3608 = spark.read \
    .schema(openaq_schema) \
    .option("header", True) \
    .option("recursiveFileLookup", True) \
    .option("pathGlobFilter", "*.csv") \
    .csv(OPENAQ_RAW + "/locationid=3608/year=2022/month=09")
files_0922 = [f.split("data/raw/")[-1] for f in check_3608.inputFiles() if "20220922" in f]
print("Files read for station 3608 on 2022-09-22:", files_0922)
print("Rows read from these files:", check_3608.withColumn("file", F.input_file_name()).filter(F.col("file").contains("20220922")).count())

openaq = openaq_raw \
    .withColumn("datetime_utc", F.to_timestamp(F.col("datetime"), DATETIME_FORMAT)) \
    .withColumn("year", F.year(F.col("datetime_utc")))

openaq.repartition("year").write.mode("overwrite").partitionBy("year").parquet(OPENAQ_OUT)

weather_raw = spark.read \
    .schema(weather_schema) \
    .option("header", True) \
    .option("recursiveFileLookup", True) \
    .option("pathGlobFilter", "*.csv") \
    .csv(WEATHER_RAW)
print("Weather CSV files read:", len(weather_raw.inputFiles()))

weather = weather_raw \
    .withColumn("datetime_utc", F.to_timestamp(F.col("datetime_utc"), DATETIME_FORMAT)) \
    .withColumn("year", F.year(F.col("datetime_utc")))

weather.repartition("year").write.mode("overwrite").partitionBy("year").parquet(WEATHER_OUT)

stations = spark.read \
    .schema(stations_schema) \
    .option("header", True) \
    .csv(STATIONS_RAW)

stations.coalesce(1).write.mode("overwrite").parquet(STATIONS_OUT)

openaq_parquet = spark.read.parquet(OPENAQ_OUT)
weather_parquet = spark.read.parquet(WEATHER_OUT)
stations_parquet = spark.read.parquet(STATIONS_OUT)

summary("OPENAQ", openaq_parquet)
openaq_parquet.groupBy("parameter", "units").count().orderBy(F.desc("count")).show()

summary("WEATHER", weather_parquet)

print("=" * 70)
print("STATIONS")
print("=" * 70)
stations_parquet.printSchema()
print("Rows:", stations_parquet.count())
print("Distinct stations:", stations_parquet.select("location_id").distinct().count())
stations_parquet.show(5, truncate=False)

print("=" * 70)
print("UTC CONVERSION AROUND DAYLIGHT SAVING TIME (station 4084, no2)")
print("=" * 70)
dst_example = openaq_parquet.filter((F.col("location_id") == 4084) & (F.col("parameter") == "no2"))
dst_example.filter(F.col("datetime_utc").between("2025-03-29 22:00:00", "2025-03-30 04:00:00")) \
    .select("datetime", "datetime_utc", "value").orderBy("datetime_utc").show(truncate=False)
dst_example.filter(F.col("datetime_utc").between("2025-10-25 21:00:00", "2025-10-26 04:00:00")) \
    .select("datetime", "datetime_utc", "value").orderBy("datetime_utc").show(truncate=False)
print("Same sensor and same UTC hour on several rows:",
      openaq_parquet.groupBy("sensors_id", "datetime_utc").count().filter(F.col("count") > 1).count())

print("=" * 70)
print("SIZES (MB)")
print("=" * 70)
print("Raw OpenAQ:", size_mb(OPENAQ_RAW))
print("Raw weather:", size_mb(WEATHER_RAW))
print("Raw stations:", size_mb(STATIONS_RAW))
print("Parquet OpenAQ:", size_mb(OPENAQ_OUT))
print("Parquet weather:", size_mb(WEATHER_OUT))
print("Parquet stations:", size_mb(STATIONS_OUT))

print("Ingestion time (s):", round(time.time() - start_time, 1))

spark.stop()
