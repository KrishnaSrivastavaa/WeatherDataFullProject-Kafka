import psycopg2
from confluent_kafka import Consumer, KafkaError
import json
import boto3
from botocore.exceptions import NoCredentialsError

# Define Kafka consumer configuration
kafka_config = {
    'bootstrap.servers': 'localhost:9092',
    'group.id': 'weatherdata-consumer',
    'auto.offset.reset': 'earliest'
}

# Create a Kafka consumer instance
consumer = Consumer(kafka_config)
consumer.subscribe(['weatherdata'])

# Define AWS S3 configuration
aws_s3_config = {
    'aws_access_key_id': 'Your-access-key-id',
    'aws_secret_access_key': 'Your-secret-access-key',
    'region_name': 'us-east-2',
    'bucket_name': 'kafkabuckett'
}

# Define a function to create the Postgres table if it doesn't exist
def create_postgres_table():
    connection = psycopg2.connect(
        database="postgres",
        user="postgres",
        password="root",
        host="localhost",
        port="5432"
    )
    cursor = connection.cursor()

    create_table_query = """
    CREATE TABLE IF NOT EXISTS weather_data (
        location_name VARCHAR(255) PRIMARY KEY,
        country VARCHAR(255),
        lat FLOAT,
        lon FLOAT,
        localtime_epoch BIGINT,
        localtime_text VARCHAR(255),
        temp_c FLOAT,
        temp_f FLOAT,
        condition_text VARCHAR(255),
        wind_mph FLOAT,
        wind_kph FLOAT,
        precip_mm FLOAT,
        precip_in FLOAT,
        gust_mph FLOAT,
        gust_kph FLOAT
    )
    """
    
    cursor.execute(create_table_query)
    connection.commit()
    connection.close()

# Define a function to insert data into the Postgres table
def insert_into_postgres(data):
    connection = psycopg2.connect(
        database="postgres",
        user="postgres",
        password="root",
        host="localhost",
        port="5432"
    )
    cursor = connection.cursor()

    insert_query = """
    INSERT INTO weather_data (
        location_name,
        country,
        lat,
        lon,
        localtime_epoch,
        localtime_text,
        temp_c,
        temp_f,
        condition_text,
        wind_mph,
        wind_kph,
        precip_mm,
        precip_in,
        gust_mph,
        gust_kph
    ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
    """
    
    cursor.execute(
        insert_query, (
            data['location']['name'],
            data['location']['country'],
            data['location']['lat'],
            data['location']['lon'],
            data['location']['localtime_epoch'],
            data['location']['localtime'],
            data['current']['temp_c'],
            data['current']['temp_f'],
            data['current']['condition']['text'],
            data['current']['wind_mph'],
            data['current']['wind_kph'],
            data['current']['precip_mm'],
            data['current']['precip_in'],
            data['current']['gust_mph'],
            data['current']['gust_kph']
        )
    )
    
    connection.commit()
    connection.close()

# Define a function to upload data to AWS S3
def upload_to_s3(data):
    s3 = boto3.client(
        's3',
        aws_access_key_id=aws_s3_config['aws_access_key_id'],
        aws_secret_access_key=aws_s3_config['aws_secret_access_key'],
        region_name=aws_s3_config['region_name']
    )

    try:
        # Convert data to JSON string
        json_data = json.dumps(data)

        # Upload to S3 with a unique key (you can customize the key)
        s3.put_object(Bucket=aws_s3_config['bucket_name'], Key=f'data/{data["location"]["name"]}.json', Body=json_data)
        print(f"Uploaded data for {data['location']['name']} to S3")
    except NoCredentialsError:
        print("Credentials not available")

# Create the Postgres table if it doesn't exist
create_postgres_table()

# Consume and process data from Kafka
while True:
    msg = consumer.poll(1.0)

    if msg is None:
        continue

    if msg.error():
        if msg.error().code() == KafkaError._PARTITION_EOF:
            print(f"Reached end of partition {msg.partition()}")
        else:
            print(f"Error while consuming: {msg.error()}")
    else:
        # Check if the message value is not empty
        if msg.value() is not None and msg.value():
            try:
                data = json.loads(msg.value())
                insert_into_postgres(data)
                upload_to_s3(data)
            except json.JSONDecodeError as e:
                print(f"Error decoding JSON: {e}")
        else:
            print("Empty or None message received")
