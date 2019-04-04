package it.utiu.anavis

import java.util.Date

class Transaction (val hash: String, val block_timestamp: Date, val inputCount: Int, val outputCount: Int, val fee: Long) extends Serializable {
  
}