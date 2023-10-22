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

    Task 2: Set up a python function that sets up a YOLOv8 instance and runs object detection on an image 

    Task 3: (dependent on Task 1): Set up a spark structured streaming cluster that reads from the kafka producer.
    For now, just verify the spark cluster can read from kafka. we will worry about integrating YOLO in Phase 2.

    Other considerations: 
        Dockerize? we may have to run this on AWS
        Broadcast the YOLOv8 model to the spark nodes so that they don't each have to set up YOLO running locally.
"""