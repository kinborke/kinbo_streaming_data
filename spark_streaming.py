from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col
from pyspark.sql.types import StructType, StructField, DoubleType, LongType

# Define the schema for the sensor data
schema = StructType([
    StructField("timestamp", LongType(), True),
    StructField("temperature", DoubleType(), True),
    StructField("humidity", DoubleType(), True),
    StructField("pressure", DoubleType(), True),
    StructField("wind_speed", DoubleType(), True),
    StructField("rainfall", DoubleType(), True)
])

# Initialize Spark session with Kafka and Cassandra packages
spark = SparkSession.builder \
    .appName("KafkaSparkStreaming") \
    .config("spark.jars.packages", "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.1,com.datastax.spark:spark-cassandra-connector_2.12:3.5.0") \
    .config("spark.cassandra.connection.host", "localhost") \
    .getOrCreate()

# Read data from Kafka
df = spark.readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "localhost:9092") \
    .option("subscribe", "sensor_data") \
    .load()

# Convert the value column from bytes to string and then to JSON
df = df.selectExpr("CAST(value AS STRING)")

# Parse the JSON data
df = df.select(from_json(col("value"), schema).alias("data")).select("data.*")

# Write data to Cassandra with checkpointing
query = df.writeStream \
    .outputMode("append") \
    .format("org.apache.spark.sql.cassandra") \
    .option("checkpointLocation", "/tmp/checkpoints") \
    .option("keyspace", "sensor_data_keyspace") \
    .option("table", "sensor_data_table") \
    .start()

query.awaitTermination()
