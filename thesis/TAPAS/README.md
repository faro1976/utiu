# TAPAS - Timely Analytics and Predictions Actor System

## Overview
A framework to execute machine learning and analytics tasks supported by actor model and in timely fashion. Based on Akka toolkit and implemented by Scala programming language, it adopts Apache Spark to compute parallel distributed analysis and Apache Kafka to decouple the layers of producer and consumer. 

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
The solution is composed of the following components:
* ingestion [producer] component: gets data collection raw data files and send message to relative topic;
* routing [consumer] compoment: reads data from topic and dispatcth message to the suitable component;
* ml learning and data analytics [trainer]: executes a ml training phase reading raw data from a distributed file system, builds a ml model and computes advanced analytics;
* prediction [predictor]: receive a request for data prediction and reply to it applyng a suitable ml model.

## Abstract implementation
TODO describe abstraction, classes and roles

TODO
