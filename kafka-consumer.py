from confluent_kafka import Consumer, KafkaError, KafkaException
import json, ssl, smtplib

# Import the email modules we'll need
from email.mime.text import MIMEText
from email.mime.multipart import MIMEMultipart
from datetime import datetime

topic_name = 'twitter_tweets'
bootstrap_servers = "kafka-bootstrap.lab.test:443"
protocol = 'SSL'
sasl_password = 'password'
ssl_CA_location = r'/home/priel/ca.pem'
group_id = 'uriel_twitter'
auto_offset_reset = 'latest'
default_topic_config = {'auto.offset.reset': auto_offset_reset}
auto_commit = True
topics = [topic_name]
min_offsets_commit = 10
consume_callback_max_messages = 0
polling_seconds_timeout = 0.1
fetch_message_max_bytes = 1048576
max_poll_interval_ms = 90000 # 300000
fetch_max_bytes = 52428800 # 52428800
auto_commit_interval_ms = 5000 # 5000
fetch_min_bytes = 1
heartbeat_interval_ms = 3300 # 3000
fetch_wait_max_ms = 500
running = True
#email features
sender_email = "urielchikohen@gmail.com"
receiver_email = "urielchikohen@gmail.com"
message = ""

def send_email(json_data):
    timestamp = str(datetime.now())
    msg = MIMEText(json_data)
    msg['From'] = sender_email
    msg['To'] = receiver_email
    msg['Subject'] = 'Kafka lab msg {}'.format(timestamp)
    s = smtplib.SMTP('email')
    s.send_message(msg)
    s.quit()

# def complete_commit(error, partitions):
#     if error:
#         print(str(error))
#     else:
#         print("commited partition offsets:"+str(partitions))


config_prop_dict = {'group.id': topic_name,
                    'bootstrap.servers': bootstrap_servers,
                    'auto.offset.reset': 'earliest',
                    'security.protocol': protocol,
                    'ssl.ca.location': ssl_CA_location,
                    'default.topic.config': default_topic_config,
                    'enable.auto.commit': auto_commit,
                    'consume.callback.max.messages': consume_callback_max_messages,
                    'fetch.message.max.bytes': fetch_message_max_bytes,
                    'max.poll.interval.ms': max_poll_interval_ms,
                    'fetch.max.bytes': fetch_max_bytes,
                    'auto.commit.interval.ms': auto_commit_interval_ms,
                    'fetch.max.bytes': fetch_max_bytes,
                    'heartbeat.interval.ms': fetch_wait_max_ms}
                    # 'on_commit': complete_commit
                    #'on_commit': complete_commit }

consumer = Consumer(config_prop_dict)
print("Consumer Started")
def consume(consumer, topics):
    consumer.subscribe(topics)
    while running:
        #msg = consumer.poll(timeout=polling_seconds_timeout)
        msg = consumer.poll(10.0)
        if msg is None: continue
        if msg.error(): continue
        print("a")
        msg = msg.value().decode('utf-8')
        send_email(json.loads(msg))

        print('msg: {}'.format(msg))
        # if msg.error():
        #     if msg.error().code() == kafkaError._PARTITION_EOF
        #     if ms
        # msg = msg.value()
        # print("Message is: ", msg)
        print("msg: ", json.loads(msg))
        consumer.commit()
    consumer.close()
        #server_ssl.quit()
    print("bye")
#running
consume(consumer, topics)
# while True:
#     msg = consumer.poll(1.0)
#     if msg is None:
#         continue
#     if msg.error():
#         print("Consumer error: {}".format(msg.error()))
#         continue
#     print('Recieved message: {}'.format(msg.value().decode('utf-8')))