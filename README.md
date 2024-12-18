# YouTube-Stream
## Description
This project implements a real-time data processing pipeline that simulates sensor data, streams it through Apache Kafka, stores it in a MySQL database, and processes the data in batches.
## Features
### Sensor Data Simulation
#### 1.Generates synthetic sensor data, including:
  1.Sensor ID
  2.Temperature
  3.Timestamp
  
#### 2.Data Streaming with Kafka
The sensor data is streamed to a Kafka topic for real-time data handling.

#### 3.Data Storage in MySQL
Kafka consumer stores the streamed data into a MySQL database.

#### 4.Batch Processing
Data is retrieved in batches from MySQL for further processing and analysis.
