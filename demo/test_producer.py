from confluent_kafka import Producer

def delivery_report(err, msg):
    # pass
    if err is not None:
        print('Message delivery failed: {}'.format(err))
    else:
        print('Message delivered to {} [{}]'.format(msg.topic(), msg.partition()))

def produce_message():
    p = Producer({'bootstrap.servers': 'localhost:29092'})

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