# from pyspark.sql import SparkSession
# from pyspark.sql.functions import from_json, col
# from pyspark.sql.types import StructType, StringType

# def kafka_consumer():
#     spark = SparkSession.builder \
#         .appName("KafkaConsumerWithSpark") \
#         .getOrCreate()

#     if spark:
#         print(f'conntected to spark')


#     df = spark.readStream \
#         .format("kafka") \
#         .option("kafka.bootstrap.servers", "localhost:9092") \
#         .option("subscribe", "test") \
#         .option("includeHeaders", "true") \
#         .load()
    
#     df.selectExpr("CAST(key AS STRING)", "CAST(value AS STRING)", "headers")
 
#     spark.stop()

# if __name__ == "__main__":
#     kafka_consumer()

from pyspark.sql import SparkSession
from pyspark.sql.functions import expr
from confluent_kafka import Consumer

def initialize_consumer():
    consumer_config = {
        'bootstrap.servers': 'localhost:9092',
        'group.id': 'my_consumer_group',
        'auto.offset.reset': 'earliest'
    }
    return Consumer(consumer_config)

def consume_message(consumer):
    consumer.subscribe(['test'])

    while True:
        msg = consumer.poll(1.0)
        if msg is None:
            continue
        if msg.error():
            print("Consumer error: {}".format(msg.error()))
            continue

        print('Received: {}'.format(msg.value().decode('utf-8')))

def main():
    spark = SparkSession.builder \
        .appName("KafkaConsumer") \
        .getOrCreate()

    consumer = initialize_consumer()
    consume_message(consumer)

    spark.stop()

if __name__ == "__main__":
    main()
