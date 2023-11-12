from confluent_kafka import Producer
import json


# Define the stats callback function
def stats_callback(stats_json_str):
    stats = json.loads(stats_json_str)
    # You can process the stats object here (e.g., log them, send them to a monitoring system, etc.)
    with open('anomaly-producer.json', 'w+') as file:
        file.write(json.dumps(stats, indent=2))

kafka_config = {
    'bootstrap.servers': 'localhost:29092',
    'stats_cb': stats_callback,  # Set the stats callback
    'statistics.interval.ms': 10000
}
producer = Producer(kafka_config)

kafka_topic = 'json_csv_topic'

csv_file_path = './art_daily_jumpsup.csv'

while True:
    try:
        with open(csv_file_path, 'r') as csv_file:
            header = []
            for line_number, line in enumerate(csv_file):
                line_data = line.strip().split(',')

                if line_number == 0:
                    header = [l.strip() for l in line_data]
                    continue

                csv_json = {header[i]: line_data[i] for i in range(len(header))}

                producer.produce(kafka_topic, value=json.dumps(csv_json).encode('utf-8'))
                # print(f'message sent: {csv_json}')
    except KeyboardInterrupt:
        print("Ended by user")
        break
    finally:
        producer.flush()


