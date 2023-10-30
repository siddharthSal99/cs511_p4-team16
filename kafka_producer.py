from pytube import YouTube
import cv2
from confluent_kafka import Producer

kafka_config = {
    'bootstrap.servers': 'localhost:9092',
}
producer = Producer(kafka_config)

# Define your YouTube URL and Kafka topic
youtube_url = "https://www.youtube.com/watch?v=IUN664s7N-c"
kafka_topic = 'youtube_streaming'

# Create a Pytube YouTube object
video = YouTube(youtube_url)
video_title = video.title
print(f"Video Title: {video_title}")

# Get the video stream
stream = video.streams.get_highest_resolution()
video_stream_url = stream.url

# Open the video stream
capture = cv2.VideoCapture(video_stream_url)
print('Video capture started')

# producer.produce(kafka_topic, value=video_title)
# print('message sent')
# producer.poll(0)

while True:
    grabbed, frame = capture.read()

    if not grabbed:
        break

    frame_bytes = frame.tobytes()

    # Produce the video frame data to Kafka
    producer.produce(kafka_topic, key='nature', value=video_title)
    print('message sent')
    producer.poll(0)


capture.release()


producer.flush()

