from pytube import YouTube
import cv2
from confluent_kafka import Producer

def callback(err, event):
    if err:
        print(f'Produce to topic {event.topic()} failed for event: {event.key()}')
    else:
        val = event.value().decode('utf8')
        print(f'{val} sent to partition {event.partition()}.')

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
# print(f"Video Title: {video_title}")

# Get the video stream
stream = video.streams.get_highest_resolution()
video_stream_url = stream.url


while True:
# Open the video stream
    capture = cv2.VideoCapture(video_stream_url)
    # print('Video capture started')

    # producer.produce(kafka_topic, value=video_title)
    # print('message sent')
    # producer.poll(0)
    frame_width = 640
    frame_height = 480
    capture.set(3, frame_width)
    capture.set(4, frame_height)



    grabbed, frame = capture.read()

    if not grabbed:
        break

    resized_frame = cv2.resize(frame, (frame_width, frame_height))

    frame_bytes = resized_frame.tobytes()

    producer.produce(kafka_topic, key='nature', value=video_title, on_delivery=callback)
    # print('message sent')
    producer.poll(0)


capture.release()


producer.flush()

