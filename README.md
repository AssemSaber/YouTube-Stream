# **YouTube-Stream**

## **Description**  
This project implements a real-time data processing pipeline to simulate sensor data, stream it using Apache Kafka, store it in a MySQL database, and process the data in batches for further analysis.

## **Features**

### **1. Sensor Data Simulation**  
The system generates **synthetic sensor data** with the following fields:  

- **Sensor ID**: A unique identifier for the sensor.  
- **Video_ID**: A unique identifier for the video.  
- **Channel_Name**: The name of the channel hosting the video.  
- **Country**: The country of origin for the channel or video.  
- **Category**: The content category (e.g., Technology, Education, Gaming).  
- **Comments**: The number of comments received by the video.  
- **Subscribers**: The number of subscribers for the channel.  
- **Views_Video**: The total number of views for the video.  
- **Likes**: The number of likes the video has received.  

### **2. Data Streaming with Kafka**  
The synthetic sensor data is streamed in real-time to a Kafka topic, enabling seamless data ingestion and high-throughput event handling.  

### **3. Data Storage in MySQL**  
A Kafka consumer retrieves the streamed data and stores it in a MySQL database for persistence and query capabilities.  

### **4. Batch Processing**  
Stored data is fetched from the MySQL database in batches to facilitate further processing and analysis, supporting some analytics. 

## **Database Setup**
```
CREATE TABLE YouTubeLive (
    Video_ID VARCHAR(30),
    Channel_Name VARCHAR(30),
    Country VARCHAR(30),
    Category VARCHAR(30),
    Comments INT,
    Subscribers INT,
    Views_Video INT,
    Likes INT
);
```
## **Steps to run code**
- **Start Zookeeper**
```
.\bin\windows\zookeeper-server-start.bat config\zookeeper.properties
```
- **Start Kafka Server**
```
.\bin\windows\kafka-server-start.bat .\config\server.properties
```
- **Create Topic**
```
kafka-topics.bat --create --bootstrap-server localhost:9092 --topic lastStream
```
- **Open Producer**
 ```
  kafka-console-producer.bat --broker-list localhost:9092 --topic lastStream
```
-  **Open Consumer**
```
  kafka-console-consumer.bat --topic lastStream --bootstrap-server localhost:9092
```
