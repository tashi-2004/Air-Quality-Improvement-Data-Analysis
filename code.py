import requests
import numpy as np
import pandas as pd
import seaborn as sns
import geopandas as gpd
import matplotlib.pyplot as plt
from shapely.geometry import Point
from pyspark.sql import SparkSession
from pyspark.sql.window import Window
from pyspark.sql import functions as F
from pyspark.ml.clustering import KMeans
from pyspark.sql.functions import row_number, monotonically_increasing_id
from pyspark.ml.feature import VectorAssembler
from pyspark.sql.types import IntegerType, FloatType
from pyspark.sql.types import StructType, StructField, StringType, FloatType, ArrayType, IntegerType
from pyspark.sql.functions import col, explode, when, udf, desc, avg, lag, to_date, count, min as spark_min, max as spark_max

#`````````````````````````````````````````````````````````````````` Spark Session ``````````````````````````````````````````````````````
spark = SparkSession.builder \
    .appName("Air Quality Improvement Analysis") \
    .getOrCreate()

sensor_schema = StructType([
    StructField("value_type", StringType(), True),
    StructField("value", StringType(), True)
])

schema = StructType([
    StructField("location", StructType([
        StructField("country", StringType(), True),
        StructField("latitude", StringType(), True),
        StructField("longitude", StringType(), True)
    ])),
    StructField("timestamp", StringType(), True),
    StructField("sensordatavalues", ArrayType(sensor_schema), True)
])

url = "https://data.sensor.community/static/v2/data.24h.json"
response = requests.get(url).json()
df = spark.createDataFrame(response, schema=schema)

data = df.select(
    "location.country",
    "location.latitude",
    "location.longitude",
    "timestamp",
    explode("sensordatavalues").alias("sensordata")
).select(
    col("country"),
    col("latitude").cast(FloatType()),
    col("longitude").cast(FloatType()),
    to_date("timestamp").alias("date"),
    col("sensordata.value").cast(FloatType()).alias("value"),
    col("sensordata.value_type")
)
#`````````````````````````````````````````````````````````````Calculating AQI ``````````````````````````````````````````````````````
def calculate_aqi(value):
    if value is None:
        return None
    if 0 <= value <= 11:
        return 1
    elif 12 <= value <= 23:
        return 2
    elif 24 <= value <= 35:
        return 3
    elif 36 <= value <= 41:
        return 4
    elif 42 <= value <= 47:
        return 5
    elif 48 <= value <= 53:
        return 6
    elif 54 <= value <= 58:
        return 7
    elif 59 <= value <= 64:
        return 8
    elif 65 <= value <= 70:
        return 9
    else:
        return 10

#`````````````````````````````````````````````````````````````````` (Task 1) `````````````````````````````````````````````````````````
aqi_udf = udf(calculate_aqi, IntegerType())
data = data.withColumn("AQI", aqi_udf(col("value")))
country_aqi = data.groupBy("country", "date").agg(avg("AQI").alias("daily_aqi"))

#`````````````````````````````````````````````````````````````````` Average AQI ``````````````````````````````````````````````````````
window_spec = Window.partitionBy("country").orderBy("date")
country_aqi = country_aqi.withColumn("prev_day_aqi", lag("daily_aqi", 1).over(window_spec))
country_imp = country_aqi.withColumn("AQI_Improvement", col("prev_day_aqi") - col("daily_aqi"))
top_countries = country_imp.orderBy(desc("AQI_Improvement")).groupBy("country").agg(
    avg("daily_aqi").alias("Current_Average_AQI")
).orderBy(desc("Current_Average_AQI"))
top_countries = top_countries.withColumn("Index", monotonically_increasing_id()+1).select("Index", "country", "Current_Average_AQI")
top_countries = top_countries.withColumnRenamed("country", "  Country     ").withColumnRenamed("Current_Average_AQI", "  Current_Average_AQI     ")

print("\t\t\t\t\t______________________________________________________")
print("\t\t\t\t\t|  TOP 10 COUNTRIES IN TERMS OF AVERAGE AIR QUALITY  |")
print("\t\t\t\t\t|____________________________________________________|")
print("\n")
top_countries.show(10, truncate=False)

#``````````````````````````````````````````````````````````````````` (Task 2) ``````````````````````````````````````````````````````````
assembler = VectorAssembler(inputCols=["latitude", "longitude"], outputCol="features")
data = assembler.transform(data)
#``````````````````````````````````````````````````````````````` KMEAN Clustering Algo ```````````````````````````````````````````````````
k_means = KMeans(k=100, seed=1)
model = k_means.fit(data)
data = model.transform(data).withColumnRenamed("prediction", "region")
region_aqi = data.groupBy("region", "date").agg(avg("AQI").alias("daily_aqi"))
region_imp = region_aqi.withColumn("prev_day_aqi", lag("daily_aqi", 1).over(Window.partitionBy("region").orderBy("date")))
region_imp = region_imp.withColumn("AQI_Improvement", col("prev_day_aqi") - col("daily_aqi"))
top_regions = region_imp.orderBy(desc("AQI_Improvement")).groupBy("region").agg(
    avg("daily_aqi").alias("Current_Average_AQI")
).orderBy(desc("Current_Average_AQI"))
top_regions = top_regions.withColumn("Index", monotonically_increasing_id()+1).select("Index", "region", "Current_Average_AQI")
top_regions = top_regions.withColumnRenamed("region", "  Region     ").withColumnRenamed("Current_Average_AQI", "  Current_Average_AQI   ")

print("\n\n")
print("--------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------")
print("\t\t\t\t\t________________________________________________")
print("\t\t\t\t\t|    TOP 50 REGIONS IN TERMS OF AIR QUALITY    |")
print("\t\t\t\t\t|______________________________________________|")
print("\n")
top_regions.show(50, truncate=False)

#`````````````````````````````````````````````````````````````````` (Task 3) `````````````````````````````````````````````````````````
good_data = data.filter(col("AQI").between(1, 3))
streak_w = Window.partitionBy("country").orderBy("date")
good_data = good_data.withColumn("is_good_day", when(col("AQI").between(1, 3), 1).otherwise(0))
streaks = good_data.withColumn("streak_id", F.sum(when(col("is_good_day") == 0, 1).otherwise(0)).over(streak_w))
streak_lengths = streaks.filter(col("is_good_day") == 1) \
    .groupBy("country", "streak_id") \
    .agg(F.count("is_good_day").alias("streak_length"))
longest_streaks = streak_lengths.groupBy("country").agg(F.max("streak_length").alias("max_streak_length"))

#`````````````````````````````````````````````````````````````````` Histogram `````````````````````````````````````````````````````````
def calculate_longest_streaks(data):
    good_data = data.filter(col("AQI").between(1, 3))
    streak_w = Window.partitionBy("country").orderBy("date")
    good_data = good_data.withColumn("is_good_day", when(col("AQI").between(1, 3), 1).otherwise(0))
    streaks = good_data.withColumn("streak_id", F.sum(when(col("is_good_day") == 0, 1).otherwise(0)).over(streak_w))
    streak_lengths = streaks.filter(col("is_good_day") == 1) \
        .groupBy("country", "streak_id") \
        .agg(F.count("is_good_day").alias("streak_length"))
    longest_streaks = streak_lengths.groupBy("country").agg(F.max("streak_length").alias("max_streak_length"))
    return longest_streaks

longest_streaks = calculate_longest_streaks(data)
print("\n\n")
print("--------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------")
colors = sns.color_palette("pastel")
hist_data = longest_streaks.select("max_streak_length").toPandas()
max_streak = hist_data["max_streak_length"].max()

plt.figure(figsize=(20, 8), facecolor='#f7f7f7')
bins = np.linspace(0, max_streak, 20)
bin_counts, _, _ = plt.hist(hist_data["max_streak_length"], bins=bins, edgecolor='lightgray', color=colors[2], alpha=0.9)
plt.title('Distribution of Longest Streaks of Good Air Quality (AQI 1-3)', fontsize=18, fontweight='bold', color='#333333')
plt.xlabel('Longest Streak of Good Air Quality (Days)', fontsize=12, color='#333333')
plt.ylabel('Frequency of Streak Lengths', fontsize=12, color='#333333')
plt.xticks(bins, rotation=45, color='#333333')
plt.yticks(color='#333333')
plt.xlim(0, max_streak)
plt.ylim(0, np.max(bin_counts) + 5)
for i, count in enumerate(bin_counts):
    if count > 0:
        plt.text(bins[i] + (bins[1] - bins[0]) / 2, count + 0.5, str(int(count)), ha='center', fontsize=11, color='#333333')
plt.tight_layout()
plt.show()
print("\n\n\n")
#``````````````````````````````````````````````````````````````````  ````````Location`````````````````````````````````````````````````
gdf = gpd.GeoDataFrame(
    data.toPandas(),
    geometry=gpd.points_from_xy(data.toPandas().longitude, data.toPandas().latitude),
    crs="EPSG:4326"
)

world = gpd.read_file('ne_110m_admin_0_countries.shp')  # Give the path you have for it. Also put all the files in same directory. 
fig, ax = plt.subplots(figsize=(18,12))
world.boundary.plot(ax=ax, linewidth=1)
gdf.plot(ax=ax, marker='o', color='orange', markersize=5, alpha=0.6, label='AQI Data Points')
plt.title('Geographical Locations of Regions with AQI Data', fontsize=18)
plt.xlabel('Longitude', fontsize=12)
plt.ylabel('Latitude', fontsize=12)
plt.tight_layout()
plt.show()
