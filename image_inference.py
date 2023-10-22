import cv2
import numpy as np
from ultralytics import YOLO

# Load the model
model = YOLO('yolov8n.pt')

# Read the image
image = cv2.imread('./dog.jpeg')

# Detect objects in the image
detections = model.predict(image)

# detections = model.predict(source='https://media.roboflow.com/notebooks/examples/dog.jpeg',
#                            conf=0.25)

result = detections[0]
# Print the detections
for box in result.boxes:
  class_id = box.cls[0].item()
  print(result.names[class_id])