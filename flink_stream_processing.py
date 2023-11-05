import cv2
import numpy as np
from pyflink.common.serialization import SimpleStringSchema
from pyflink.datastream import StreamExecutionEnvironment
from pyflink.datastream.connectors import FlinkKafkaConsumer
# import torch

# Load YOLOv5 model
# model = torch.hub.load('ultralytics/yolov5', 'yolov5s')

def process_image(img_str):
    # # Convert the string data to a NumPy array
    # np_img = np.frombuffer(img_str.encode(), np.uint8)
    
    # # Decode the image
    # img = cv2.imdecode(np_img, cv2.IMREAD_COLOR)
    
    # # Run YOLOv5 inference
    # results = model(img)
    # return results
    return img_str

def main():
    # Set up the Flink environment
    env = StreamExecutionEnvironment.get_execution_environment()
    env.add_jars("file:///C:/Users/siddh/Documents/Projects/Masters/UIUC/CS511/cs511p4/cs511_p4-team16/flink-sql-connector-kafka-1.15.0.jar")

    # Define the Kafka properties and topic
    properties = {
        "bootstrap.servers": "localhost:9092",
        "group.id": "image_consumer"
    }
    topic = "youtube_streaming"

    # Create a Kafka consumer for Flink
    kafka_consumer = FlinkKafkaConsumer(topics=topic, deserialization_schema=SimpleStringSchema(), properties=properties)
    kafka_consumer.set_start_from_earliest()

    # Add the Kafka consumer to the data stream
    image_stream = env.add_source(kafka_consumer)

    # Process the image data using YOLOv5
    results_stream = image_stream.map(process_image)

    # Print the results (or further process as required)
    results_stream.print()

    # Execute the Flink job
    env.execute()

if __name__ == '__main__':
    main()