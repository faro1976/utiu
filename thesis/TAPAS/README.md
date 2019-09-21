# TAPAS - Timely Analytics and Predictions Actor System

Transaction features for confirmation time prediction:
- hour of day (0-23)
- mempool size (byte)
- mempool quantity (txs total number)
- fee (sat)
- is segwit (boolean)
- btc price (joined with bitcoinstamp dataset)
- tx size
- tx out vol
- in/out total number


# Scheduled activities
A training restart after 30 seconds of finishing
....




# CASAkka - Concurrent Agent Simulation with AKKA 

## Overview
A framework for large scale agent-based model simulation upon actor-model and Akka; realistic modelling (as much as possibile…) thanks to asynchronous events and message-passing communication way, independent agent life-cycle, business/technology decoupling, agent strategy logic coding with any JVM language.


## Installation
* pull-down github repository

```shell
git clone https://github.com/bancaditalia/ABMS
```

* go to CASAKKA home folder

```shell
cd ABMS/largeScaleSim/akka/simpleMarketModel
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


## Configuration
Copy configuration file <code>src/main/resources/application.conf.sample</code> into <code>cfg</code> directory; change it according your market simulation variables (see [a sample configuration file](cfg/application.conf.sample)).

```shell 
vi cfg/application.conf
```

### application.conf
use following notation:

* key1=value1					_for single value_
* key1="[value1,value2]"		_for list of values_
* key1="[value1:value2,value3]"	_for range of values, starting from value1 to value2 increment by value3_
pay attention to double quotes!!

| Parameter | Description |
| ------ | ----------- |
| simple-market-model.simulation.simulations   | number of simulations per experiment |
| simple-market-model.simulation.ticks   | number of tick per simulation |
| simple-market-model.simulation.tick-duration   | tick duration (msecs) |
| simple-market-model.simulation.tick-driven   | simulation events approach (true | false) |
|    |  |
| monitoring.enabled   | monitor enabled (true | false) |
| monitoring.interval   | agents monitoring period (msecs) |
| monitoring.event-collector   | event collector enabled (true | false) |
|    |  |
| strategy.class-name   | list of strategy classes fully qualified name |
| strategy.approach   | list of approaches |
|    |  |
| shock.interval   | shock raising period (msecs) |
| shock.prob   | shock propability (0-1) |
| shock.value.mean   | shock value mean |
| shock.value.stddev   | shock value standard deviation |
|      |  |
| asset.collect.interval   | collect period (msecs) |
| asset.collect.prob   | collect probability (0-1) |
| asset.collect.dividend.mean   | collect value mean |
| asset.collect.dividend.stddev   | collect value standard deviation |
| asset.value   | asset value |
| asset.tot-stocks   | total number of asset stocks available |
|    |  |
| agent.currency-single-amount   | agent initial currency amount |
| agent.tot-number   | total number of agents |
| agent.send-order-prob   | agent order sending probability (0-1) |
|      |  |
| market.trend-last-n-max   | last n elements of ticks to retrieve |





## Execution
Run experiment in:

* multi-simulation mode ([1]experiment->[1..n]sessions->[1..n]simulations->[1-n]ticks)

```shell
dist/start-multisim.sh
``` 

* single-simulation mode

```shell 
dist/start-singlesim.sh
```


## Log analysis
Look at folder <code>log</code> for the csv results of the experiment.


## Strategy implementation
Code your own strategy implementing <code>it.bdi.art.smm.strategy.TStrategy</code> trait adopting <code>it.bdi.art.smm.strategy</code> package; the method <code>TStrategy.play</code> must return an instance of <code>it.bdi.art.smm.vo.OrderStrategyVO</code>.
Add strategy class name to <code>cfg/application.conf</code> by <code>strategy.class-name</code> key.
Build your jar and place it into <code>lib</code> folder changing the script to launch simulation in order to include the jar into the classpath.


Below a sample of strategy implementation with a zero intelligence logic:
```scala
package it.bdi.art.smm.strategy

import java.util.Date
import scala.collection.mutable.Map
import scala.util.Random
import it.bdi.art.smm.actor.AgentActor
import it.bdi.art.smm.runner.SimulationApp
import it.bdi.art.smm.vo.OrderType
import it.bdi.art.smm.vo.OrderVO
import scala.collection.mutable.ListBuffer
import it.bdi.art.smm.vo.OrderStrategyVO
import it.bdi.art.smm.vo.OrderStrategyVO


object ZIStrategy {
  val MAX_UP_BOUND = 50
}


class ZIStrategy() extends TStrategy {  
  
  def play(id:String, tick:Int, currency:Float, stocks:Int, avgLast:Float, minOffer:Option[OrderVO], maxAsk:Option[OrderVO], lastPrice:Option[Float], currentAprroach:String, approachMap: Map[String, Int], lastNTickList: Array[Float]) : Option[OrderStrategyVO]={   
      val isAsk = AgentActor.makeAsk()  //random ask/offer order generator
      
      val price = (new Random().nextInt(ZIStrategy.MAX_UP_BOUND * 10) + 1) / 10.0f
      val qty = new Random().nextInt(ZIStrategy.MAX_UP_BOUND) + 1
            
      var order:Option[OrderStrategyVO] = None
      
      if (isAsk && (qty * price) <= currency.toInt) {
        //ask order
        order = Some(new OrderStrategyVO(price, qty, OrderType.Ask, ZIStrategy.getClass.getSimpleName))
      } else if(stocks >= qty) {
        //offer order
        order = Some(new OrderStrategyVO(price, qty, OrderType.Offer, ZIStrategy.getClass.getSimpleName))
      }
      
      order
  }
  
}
```

Below a list of strategy input parameters:

| Parameter | Description |
| ------ | ----------- |
| id | agent ID |
| tick | tick number |
| currency | current agent currency |
| stocks | current agent stocks |
| avgLast | average price of last N ticks |
| minOffer | current minimum offer price |
| maxAsk | current maximum ask price |
| lastPrice | last crossed price |
| currentAprroach | last approach used by current agent |
| approachMap | KV pairs approach->number of agents |
| lastNTickList | list of average prices of last N ticks |
