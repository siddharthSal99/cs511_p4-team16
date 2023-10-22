from pyspark.sql import SparkSession
from pyspark.sql.functions import udf
from pyspark.sql.types import StringType
import cv2
import numpy as np

def decode_image(value):
    """Decode the image from bytes to a format suitable for further processing."""
    # Convert bytes to numpy array
    nparr = np.frombuffer(value, np.uint8)
    
    # Decode the image
    img = cv2.imdecode(nparr, cv2.IMREAD_COLOR)
    
    # Here you can process the image further if needed
    # For simplicity, returning the shape of the image
    return str(img.shape)

# UDF to process the images after reading from Kafka
decode_image_udf = udf(decode_image, StringType())

# Initialize SparkSession
spark = SparkSession.builder \
    .appName("KafkaImageStream") \
    .getOrCreate()

# Read from Kafka
stream_df = spark.readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "your_kafka_bootstrap_server") \
    .option("subscribe", "your_topic_name") \
    .load()

# Deserialize the value (image bytes)
images = stream_df.selectExpr("CAST(value AS BINARY)")

# Process images
processed = images.withColumn("image_shape", decode_image_udf(images.value))

# For demo purposes, just write the image shape to the console
query = processed.writeStream \
    .outputMode("append") \
    .format("console") \
    .start()

query.awaitTermination()