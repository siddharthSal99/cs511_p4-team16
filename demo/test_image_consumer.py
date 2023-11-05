from confluent_kafka import Consumer, KafkaError
from vaderSentiment.vaderSentiment import SentimentIntensityAnalyzer
import json
import cv2
import numpy as np
from ultralytics import YOLO


# Define the stats callback function
def stats_callback(stats_json_str):
    stats = json.loads(stats_json_str)
    # You can process the stats object here (e.g., log them, send them to a monitoring system, etc.)
    print(json.dumps(stats, indent=2))


def consume_img_kafka(consumer):
    msg = consumer.poll(1.0)
    return msg

def decode_byte_to_frame(byte):
    frame = cv2.imdecode(np.frombuffer(byte, np.uint8), -1)
    return frame

def consume_messages(model):
    c = Consumer({
        'bootstrap.servers': 'localhost:29092',
        'group.id': 'my-group',
        'auto.offset.reset': 'earliest',
        'stats_cb': stats_callback,  # Set the stats callback
        'statistics.interval.ms': 10000
    })

    c.subscribe(['youtube_streaming'])

    
    try:
        while True:
            msg = consume_img_kafka(c)

            if msg is None:
                print("None")
                continue
            if msg.error():
                if msg.error().code() == KafkaError._PARTITION_EOF:
                    print('Reached end of partition')
                else:
                    print('Error while consuming message: {}'.format(msg.error()))
            else:
                # print(msg.value)
                frame = decode_byte_to_frame(msg.value())
                detections = model.predict(frame)
                result = detections[0]
                # Print the detections
                for box in result.boxes:
                    class_id = box.cls[0].item()
                    print(result.names[class_id])
                # sentiment_scores(msg.value().decode('utf-8'))
    except KeyboardInterrupt:
        print('Canceled by user.')
    finally:
        c.close()
 
if __name__ == '__main__':
    model = YOLO('yolov8n.pt')
    consume_messages(model)