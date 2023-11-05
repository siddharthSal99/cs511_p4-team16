from confluent_kafka import Producer
from pytube import YouTube
import cv2

def delivery_report(err, msg):
    # pass
    if err is not None:
        print('Message delivery failed: {}'.format(err))
    else:
        print('Message delivered to {} [{}]'.format(msg.topic(), msg.partition()))

def encode_frame_to_byte(frame):
    # Convert frame to byte
    imgbyte = cv2.imencode('.jpg', frame)[1].tobytes()
    return imgbyte


def produce_img_kafka(producer, frame, topic):
    imgbyte = encode_frame_to_byte(frame)
    # Send image to kafka
    producer.produce(topic, value=imgbyte, callback=delivery_report)

def produce_message():
    youtube_url = "https://www.youtube.com/watch?v=IUN664s7N-c"
    kafka_topic = 'youtube_streaming'

    # Create a Pytube YouTube object
    video = YouTube(youtube_url)

    # Get the video stream
    stream = video.streams.get_highest_resolution()
    video_stream_url = stream.url
    p = Producer({'bootstrap.servers': 'localhost:29092'})

    while True:
        try:
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
            produce_img_kafka(p, resized_frame, kafka_topic)
        except KeyboardInterrupt:
            print("Killed by user")
            break
        finally:
            p.flush()

if __name__ == '__main__':
    produce_message()