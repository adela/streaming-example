# streaming-example

# Overview
 This is a Spark Streaming example that combines SparkML, SparkSQL with Spark Streaming functionality.

This example creates an unsupervised model using SparkML K-Means and afterwards,it loads the model in a Spark Streaming
job.

The input data is a set of Uber trip data with GPS coordinates, one per line.

Step1: Using K-Means clustering, the GPS positions are clustered into a number of k clusters as defined in the code.

Step2: The streaming job receives one such trip data and automatically assigns it to an existing cluster using the
model generated in step 1.


# Data
An input sample is located in data/uber.csv
Format:

Date/Time: The date and time of the Uber pickup
Lat: The latitude of the Uber pickup
Lon: The longitude of the Uber pickup
Base: The TLC base company affiliated with the Uber pickup

Uber trip data: http://data.beta.nyc/dataset/uber-trip-data-foiled-apr-sep-2014

# Main Classes

## ClusteringDriver
Step1: Generates the K-Means model. You can run the ClusteringDriver class to understand the input data, create a
model and save it.

## StreamingDriver
Step2: Start a streaming job that listens to a specific Kafka topic for String lines similar to the ones found in the
 data folder. Each position is mapped to an existing cluster as part as this job.

## Config
The class contains some placeholders for the Kafka brokers and the Kafka topic. You must have a topic already created
and one or more Kafka Brokers

val BrokersList = "<YourBrokerListsHere>" //host:port,host:port
val Topic = "<YourTopicHere>"

### Note

The code is based on the MapR example detailed <a href="https://mapr
.com/blog/monitoring-real-time-uber-data-using-spark-machine-learning-streaming-and-kafka-api-part-2/">  here </a>