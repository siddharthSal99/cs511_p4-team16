"""
Phase 1: initial parts of the system: 
    Task 1: Set up video stream as a producer for kafka:
    https://stackoverflow.com/questions/43032163/how-to-read-youtube-live-stream-using-opencv-python

    import cv2
    from kafka import KafkaProducer

    producer = KafkaProducer(bootstrap_servers='your_kafka_server:port')
    cap = cv2.VideoCapture('path_to_video.mp4')

    while cap.isOpened():
        ret, frame = cap.read()
        # Convert frame to JPEG and then to bytes
        _, buffer = cv2.imencode('.jpg', frame)
        producer.send('video_topic', buffer.tobytes())
"""