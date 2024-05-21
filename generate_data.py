import json
import random
import time
from kafka import KafkaProducer

#Create a Kafka producer
producer = KafkaProducer(
    bootstrap_servers = 'localhost:9092',
    value_serializer = lambda v : json.dumps(v).encode('utf-8')
)

#function to generate synthetic sensor data

def generate_sensor_data():
    data = {
        'timestamp': int(time.time()),
        'temperature': round(random.uniform(20, 40), 2), # Temperature in Celsius
        'humidity': round(random.uniform(40, 80), 2) , # Humidity in percentage
        'pressure': round(random.uniform(900, 1100), 2),  # Pressure in hPa
        'wind_speed': round(random.uniform(0, 10), 2),  # Wind speed in m/s
        'rainfall': round(random.uniform(0, 5), 2)  # Rainfall in mm/h
    }
    return data

# Main function to continuously generate and send sensor data to Kafka

def main():
    while True:
        data = generate_sensor_data()
        producer.send('sensor_data', value=data)
        print(f'Sent: {data}')
        time.sleep(1)

# Run the main function

if __name__ == "__main__":
    main()
