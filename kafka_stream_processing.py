from confluent_kafka import Consumer, KafkaException

def consume_messages():
    # Create a Kafka consumer instance with desired properties
    config = {
        'bootstrap.servers': 'localhost:9092',
    }
    config['group.id'] = 'my_group'
    config['auto.offset.reset'] = 'earliest'
    config['enable.auto.commit'] = False

    consumer = Consumer(config)

    consumer.subscribe(['youtube_streaming'])

    # Continuous loop to read new messages
    try:
        while True:
            event = consumer.poll(1.0)
            if event is None:
                continue
            if event.error():
                raise KafkaException(event.error())
            else:
                val = event.value().decode('utf8')
                partition = event.partition()
                print(f'Received: {val} from partition {partition}    ')
    except KeyboardInterrupt:
        print('Canceled by user.')
    finally:
        consumer.close()

if __name__ == '__main__':
    consume_messages()
