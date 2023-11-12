from confluent_kafka import Producer
import json

kafka_config = {
    'bootstrap.servers': 'localhost:29092',
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

