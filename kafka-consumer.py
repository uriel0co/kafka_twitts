import sys

from confluent_kafka import Consumer
import json, ssl, smtplib

from email.mime.text import MIMEText
from datetime import datetime
import time

# kafka features
topic_name = 'twitter_tweets'
bootstrap_servers = "kafka-bootstrap.lab.test:443"
protocol = 'SSL'
sasl_password = 'password'
ssl_CA_location = r'/home/priel/ca.pem'
group_id = 'uriel_twitter'
auto_offset_reset = 'latest'
default_topic_config = {'auto.offset.reset': auto_offset_reset}
topics = [topic_name]
min_offsets_commit = 10
consume_callback_max_messages = 0
polling_seconds_timeout = 0.1
max_partition_fetch_bytes = 1048576
max_poll_interval_ms = 90000  # 300000
fetch_min_bytes = 1
fetch_wait_max_ms = 500
session_timeout_ms = 5000

# email features
sender_email = "condumestwittsuriel@gmail.com"
receiver_email = "condumestwittsuriel@gmail.com"
password = "AAron123"
message = ""


def connect_email():
    port = 465 #For SSL
    smtp_server = 'smtp.gmail.com'
    context = ssl.create_default_context()  # A fresh context with secure default settings
    server = smtplib.SMTP_SSL(smtp_server, port, context=context)  # A secure conn to gmail SMTP server
    server.login(sender_email, password)
    return server


def send_email(data_dict, server):
    start = time.time()
    timestamp = str(datetime.now())
    body = 'Email content: {}'.format(data_dict['text'])
    msg = MIMEText(body)  # Multipurpose internet mail extension - 'Emaily' sendable object
    msg['From'] = sender_email
    msg['To'] = receiver_email
    msg['Subject'] = 'Kafka Lab - Sent On: {}'.format(timestamp)
    server.sendmail(sender_email, receiver_email, msg.as_string())
    print("email sent successfully")
    end = time.time()
    print("Inner function time: " + str(end - start))


def consume(consumer, topics):
    consumer.subscribe(topics)
    email_count = 0
    while True:
        start2 = time.time()
        msg = consumer.poll(1)  # Fetching records
        if msg is None: continue
        if msg.error(): continue
        msg = msg.value().decode('utf-8')
        msg = json.loads(msg)
        try:
            send_email(msg, server)
        except Exception as e:
            continue
        email_count += 1
        print('Email number {} successfully sent !'.format(email_count))
        consumer.commit()
        end2 = time.time()
        print("Outer function time: " + str(end2 - start2))
    consumer.close()
    print("bye")


# Produce process
 #Conecting via SSL
server = connect_email()
config_prop_dict = {'group.id': topic_name,
                    'bootstrap.servers': bootstrap_servers,
                    'auto.offset.reset': 'earliest',
                    'security.protocol': protocol,
                    'ssl.ca.location': ssl_CA_location,
                    'default.topic.config': default_topic_config,
                    'enable.auto.commit': True,
                    'consume.callback.max.messages': consume_callback_max_messages,
                    'max.partition.fetch.bytes': max_partition_fetch_bytes,
                    'max.poll.interval.ms': max_poll_interval_ms,
                    'session.timeout.ms': session_timeout_ms,
                    'heartbeat.interval.ms': session_timeout_ms * 0.3}
# Create consumer
consumer = Consumer(config_prop_dict)
print("Consumer Started")
consume(consumer, topics)