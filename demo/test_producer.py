from confluent_kafka import Producer
import json

def delivery_report(err, msg):
    # pass
    if err is not None:
        print('Message delivery failed: {}'.format(err))
    else:
        print('Message delivered to {} [{}]'.format(msg.topic(), msg.partition()))

# Define the stats callback function
def stats_callback(stats_json_str):
    stats = json.loads(stats_json_str)
    # You can process the stats object here (e.g., log them, send them to a monitoring system, etc.)
    with open('text-producer.json', 'w+') as file:
        file.write(json.dumps(stats, indent=2))

def produce_message():
    p = Producer({'bootstrap.servers': 'localhost:29092','stats_cb': stats_callback,  # Set the stats callback
    'statistics.interval.ms': 5000})

    while True:
        try:
            with open('kinglear.txt','rb') as file:
                for line in file.readlines():  
                    p.produce('king_lear_text', value=line, callback=delivery_report)
        except KeyboardInterrupt:
            print("Killed by user")
        finally:
            p.flush()

if __name__ == '__main__':
    produce_message()