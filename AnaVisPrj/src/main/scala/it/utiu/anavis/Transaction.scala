package it.utiu.anavis

import java.util.Date


//classe rappresentante la transazione Bitcoin
class Transaction (val hash: String, val timestamp: Date, val inputCount: Int, val outputCount: Int, val fee: Double, val outputValue: Double, val size: Long) extends Serializable {
  
}