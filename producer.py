# from confluent_kafka import Producer
# import time

# producer = Producer({'bootstrap.servers': 'localhost:9092'})

# for i in range(10):
#     message = "Message {}".format(i)
#     producer.produce('hello this is my first text', message.encode())
#     print("Sent: ", message)
#     # time.sleep(1)

# producer.flush()

# from confluent_kafka import Producer
# import time


# producer_config = {
#     'bootstrap.servers': 'localhost:9092'
# }
# producer = Producer(producer_config)


# message = "hello this is ritika misal"

# producer.produce('test', message.encode())
# print("Sent: ", message)


# producer.flush()

# from pyspark.sql import SparkSession
# from confluent_kafka import Producer

# def initialize_producer():
#     producer_config = {
#         'bootstrap.servers': 'localhost:9092'
#     }
#     return Producer(producer_config)

# def send_message(producer, message):
#     producer.produce('test', message.encode())
#     print("Sent: ", message)
#     producer.flush()

# def main():
#     spark = SparkSession.builder \
#         .appName("KafkaProducer") \
#         .getOrCreate()
     
#     # if spark:
#     #     print(f'conntected to spark')



#     ds = df \
#           .selectExpr("topic", "CAST(key AS STRING)", "CAST(value AS STRING)") \
#           .writeStream \
#           .format("kafka") \
#           .option("kafka.bootstrap.servers", "localhost:9092") \
#           .start()
    
# producer = initialize_producer()
# while True:
#         message = input("Enter message to send (type 'exit' to quit): ")
#         if message == 'exit':
#             break
#         send_message(producer, message)

#         spark.stop()

# if __name__ == "__main__":
#     main()

# from pyspark.sql import SparkSession
# from pyspark.sql.functions import expr
# from confluent_kafka import Producer


# def initialize_producer():
#     producer_config = {
#         'bootstrap.servers': 'localhost:9092'
#     }
#     return Producer(producer_config)


# def send_message(producer, topic, message):
#     producer.produce(topic, message.encode())
#     print(f"Sent to topic '{topic}': {message}")
#     producer.flush()


# def main():
#     spark = SparkSession.builder \
#         .appName("KafkaProducer") \
#         .getOrCreate()


#     producer = initialize_producer()


#     while True:
#         message = input("Enter message to send (type 'exit' to quit): ")
#         if message == 'exit':
#             break
#         send_message(producer, "test", message)

 
#     spark.stop()
#     producer.flush()
#     producer.close()


# if __name__ == "__main__":
#     main()




# from pyspark.sql import SparkSession

# def main():
#     spark = SparkSession.builder \
#         .appName("KafkaConsumer") \
#         .getOrCreate()

#     df = spark \
#         .read \
#         .format("kafka") \
#         .option("kafka.bootstrap.servers", "localhost:9092") \
#         .option("subscribe", "test") \
#         .load()

#     df.selectExpr("CAST(key AS STRING)", "CAST(value AS STRING)").show(truncate=False)

#     spark.stop()

# if __name__ == "__main__":
#     main()

# from pyspark.sql import SparkSession
# from pyspark.sql.functions import expr
# from confluent_kafka import Consumer, Producer


# def initialize_producer():
#     producer_config = {
#         'bootstrap.servers': 'localhost:9092'
#     }
#     return Producer(producer_config)


# def send_message(producer, topic, message):
#     producer.produce(topic, message.encode())
#     print(f"Sent to topic '{topic}': {message}")
#     producer.flush()


# def initialize_consumer():
#     consumer_config = {
#         'bootstrap.servers': 'localhost:9092',
#         'group.id': 'my_consumer_group',
#         'auto.offset.reset': 'earliest'
#     }
#     return Consumer(consumer_config)


# def consume_message(consumer):
#     consumer.subscribe(['test'])

#     while True:
#         msg = consumer.poll(1.0)
#         if msg is None:
#             continue
#         if msg.error():
#             print("Consumer error: {}".format(msg.error()))
#             continue

#         print('Received: {}'.format(msg.value().decode('utf-8')))


# def main():
#     spark = SparkSession.builder \
#         .appName("KafkaProducerConsumer") \
#         .getOrCreate()

#     producer = initialize_producer()
#     consumer = initialize_consumer()

#     consume_message(consumer)

#     while True:
#         message = input("Enter message to send (type 'exit' to quit): ")
#         if message == 'exit':
#             break
#         send_message(producer, "test", message)

#     spark.stop()
#     producer.flush()
#     producer.close()
#     consumer.close()


# if __name__ == "__main__":
#     main()

from confluent_kafka import Producer

def initialize_producer():
    producer_config = {
        'bootstrap.servers': 'localhost:9092'
    }
    return Producer(producer_config)

def send_message(producer, topic, message):
    producer.produce(topic, message.encode())
    print(f"Sent to topic '{topic}': {message}")
    producer.flush()

def main():
    producer = initialize_producer()

    while True:
        message = input("Enter message to send (type 'exit' to quit): ")
        if message == 'exit':
            break
        send_message(producer, "test", message)

    producer.flush()
    producer.close()

if __name__ == "__main__":
    main()
