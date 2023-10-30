"""
Phase 1: initial parts of the system: 
    Task 1: Set up video stream as a producer for kafka:
    https://stackoverflow.com/questions/43032163/how-to-read-youtube-live-stream-using-opencv-python

    Anomaly detection Stream Data: https://www.kaggle.com/datasets/boltzmannbrain/nab/data

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

Phase 2: Integrate the YOLOv8 code into the spark cluster
    Task 1: get yolov8 running on a local spark cluster to run inference on the kafka stream locally
    
    Task 2: run spark with the yolov8 model & kafka producer on AWS so that we can set GPU usage
"""



