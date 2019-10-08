# TAPAS - Timely Analytics and Predictions Actor System

## Overview
A framework to execute machine learning and data analytics tasks supported by actor model and in a timely fashion.
Based on Akka toolkit following actor model, implemented by Scala programming language, it adopts Apache Spark to compute parallel distributed analysis, Apache Kafka to decouple the layers of producer and consumer, HDFS for distributed stored and processing of big data.


## Installation
* pull-down github repository

```shell
git clone https://github.com/faro1976/utiu.git
```

* go to TAPAS home folder

```shell
cd thesis/TAPAS
```

* build executable (thanks to maven)
	* with test cases
	
	```shell
	mvn clean package
	``` 
	* or without test cases
	
	```shell 
	mvn -DskipTests=true clean package
	```

## Start all daemons
execute
```shell
bin/start-all.sh
```
in order to start:
* Apache ZooKeeper - distributed coordinator
* Apache Kafka - message broker
* Apache HDFS - distributed file system
* Apache Spark - distributed parallel computing

## Case studies

* Bitcoin statistics and prediction

Bitcoin price prediction and general statistics about Blockchain and Bitcoin network (regression, ? features): regression techniques to predict Bitcoin price observing a few features inside Blockchain and Bitcoin peer-to-peer network.
Dataset retrieved from regular polling of Blockchair REST APIs (https://github.com/Blockchair/Blockchair.Support/blob/master/API.md).

* Activity detection

Activity recognition of older people by wearable sensors (classification, 9 features, 4 classes): classification of older people motion data to detect motion labels: sitting on bed, sitting on chair, lying on bed, ambulating. 
Dataset from https://archive.ics.uci.edu/ml/datasets/Activity+recognition+with+healthy+older+people+using+a+batteryless+wearable+sensor 

* Wine (debug only)

Wine cultivars classification based on a chemical analysis (classification, 13 features, 3 classes).
Dataset from https://archive.ics.uci.edu/ml/datasets/Wine

## Execution
Run producer simulation in case study:

* case study Bitcoin

```shell
aaaa
``` 

* case study Activity detection

```shell
aaaa
``` 

## Pipeline
The solution is composed of the following phases [components]:
* ingestion [producer]: collects the raw data files and sends its by message to relative topic of a message broker;
* routing [consumer]: reads the data from topic and dispatcthes the message to the suitable component, choosing action between save into a distributed file system if the message is a data input and forward request to predictor if the messagge is a prediction request; 
* ml training [trainer]: executes a ml training phase reading raw data from a distributed file system, builds a ml model and notifies its predictor transferring to it the fresh model just computed;
* prediction [predictor]: receives a prediction request and replies to it applying a suitable ml model and making a prediction;
* analysis [analyzer]: computes statistics on the raw data from a distributed file system and notifies its predictor transferring to it the fresh stastistic data.


## Abstract implementation
TODO ROB: describe abstraction, classes and roles above
