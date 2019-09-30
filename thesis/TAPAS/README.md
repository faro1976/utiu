# TAPAS - Timely Analytics and Predictions Actor System

## Overview
A framework to execute machine learning and analytics algorithms supported by actor model and in a (near) real-time way. 


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

Bitcoin price prediction and general statistics about blockchain and Bitcoin network (regression, ? features): regression techniques to predict Bitcoin price observing a few features inside Blockchain and Bitcoin peer-to-peer network.
Dataset retrieved from regular polling of Blockchair REST APIs (https://github.com/Blockchair/Blockchair.Support/blob/master/API.md).

* Activity detection

Activity recognition of older people by wearable sensors (classification, 9 features, 4 classes): classification of older people motion data to detect motion label: sitting on bed, sitting on chair, lying on bed, ambulating. 
Dataset from https://archive.ics.uci.edu/ml/datasets/Activity+recognition+with+healthy+older+people+using+a+batteryless+wearable+sensor 

* Wine (debug only)

Wine cultivars classification based on a chemical analysis (classification, 13 features, 3 classes) .
dataset from https://archive.ics.uci.edu/ml/datasets/Wine

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


## Abstract implementation
TODO describe abstraction, classes and roles

TODO
