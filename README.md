# YouTube-Stream
## Description
This project implements a real-time data processing pipeline that simulates sensor data, streams it through Apache Kafka, stores it in a MySQL database, and processes the data in batches.
## Features
### Sensor Data Simulation
#### 1.Generates synthetic sensor data, including:
<div>Sensor ID: A unique identifier for the sensor</div>
Video_ID: A unique identifier for the video.
Channel_Name: The name of the channel hosting the video.
Country: The country of origin for the channel or video.
Category: The content category (e.g., Technology, Education, Gaming).
Comments: The number of comments the video has received.
Subscribers: The number of subscribers for the channel.
Views_Video: The total number of views for the video.
Likes: The number of likes the video has received.
  
#### 2.Data Streaming with Kafka
The sensor data is streamed to a Kafka topic for real-time data handling.

#### 3.Data Storage in MySQL
Kafka consumer stores the streamed data into a MySQL database.

#### 4.Batch Processing
Data is retrieved in batches from MySQL for further processing and analysis.
