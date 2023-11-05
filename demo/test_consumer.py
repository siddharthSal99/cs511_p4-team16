import pickle
from confluent_kafka import Consumer, KafkaError
from vaderSentiment.vaderSentiment import SentimentIntensityAnalyzer
import json


# Define the stats callback function
def stats_callback(stats_json_str):
    stats = json.loads(stats_json_str)
    # You can process the stats object here (e.g., log them, send them to a monitoring system, etc.)
    print(json.dumps(stats, indent=2))

def consume_messages():
    c = Consumer({
        'bootstrap.servers': 'localhost:29092',
        'group.id': 'my-group',
        'auto.offset.reset': 'earliest',
        'stats_cb': stats_callback,  # Set the stats callback
        'statistics.interval.ms': 10000
    })

    c.subscribe(['json_csv_topic'])
    # c.subscribe(['king_lear_text'])
    
    try:
        while True:
            msg = c.poll(1.0)

            if msg is None:
                print("None")
                continue
            if msg.error():
                if msg.error().code() == KafkaError._PARTITION_EOF:
                    print('Reached end of partition')
                else:
                    print('Error while consuming message: {}'.format(msg.error()))
            else:
                # print('Received message: {}'.format(msg.value().decode('utf-8')))
                
                # sentiment_scores(msg.value().decode('utf-8'))
                anomaly_detection(msg.value())
    except KeyboardInterrupt:
        print('Canceled by user.')
    finally:
        c.close()
        
def anomaly_detection(data):
    val = json.loads(data)['value']
    filename = 'clf.pkl'
    loaded_model = pickle.load(open(filename, 'rb'))
    result = loaded_model.predict([[val]])
    print(result)
def sentiment_scores(sentence):
 
    # Create a SentimentIntensityAnalyzer object.
    sid_obj = SentimentIntensityAnalyzer()
 
    # polarity_scores method of SentimentIntensityAnalyzer
    # object gives a sentiment dictionary.
    # which contains pos, neg, neu, and compound scores.
    sentiment_dict = sid_obj.polarity_scores(sentence)
     
    # print("Overall sentiment dictionary is : ", sentiment_dict)
    # print("sentence was rated as ", sentiment_dict['neg']*100, "% Negative")
    # print("sentence was rated as ", sentiment_dict['neu']*100, "% Neutral")
    # print("sentence was rated as ", sentiment_dict['pos']*100, "% Positive")
 
    print("Sentence Overall Rated As", end = " ")
 
    # decide sentiment as positive, negative and neutral
    if sentiment_dict['compound'] >= 0.05 :
        print("Positive")
 
    elif sentiment_dict['compound'] <= - 0.05 :
        print("Negative")
 
    else :
        print("Neutral")
 
if __name__ == '__main__':
    consume_messages()