# YouTube-Stream
This project implements a real-time data processing pipeline that simulates sensor data, streams it through Apache Kafka, stores it in a MySQL database, and processes the data in batches.
## Features
### Sensor Data Simulation
-Generates synthetic sensor data, including:
  -Sensor ID
  -Temperature
  -Timestamp
Data Streaming with Kafka
The sensor data is streamed to a Kafka topic for real-time data handling.

Data Storage in MySQL
Kafka consumer stores the streamed data into a MySQL database.

Batch Processing
Data is retrieved in batches from MySQL for further processing and analysis.
