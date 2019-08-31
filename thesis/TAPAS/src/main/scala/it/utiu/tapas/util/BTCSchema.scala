package it.utiu.tapas.util

import org.apache.spark.sql.types.LongType
import org.apache.spark.sql.types.IntegerType
import org.apache.spark.sql.types.DoubleType
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.types.BooleanType
import org.apache.spark.sql.types.StructField
import org.apache.spark.sql.types.StringType
import org.apache.spark.sql.types.Metadata

object BTCSchema {
    //definzione schema dataframe finale    
    val schema = new StructType()
      .add(StructField("fee", DoubleType, false, Metadata.empty))
      .add(StructField("segwit", BooleanType, false, Metadata.empty))
      .add(StructField("size", IntegerType, false, Metadata.empty))
      .add(StructField("t", LongType, false, Metadata.empty))
      .add(StructField("tfs", LongType, false, Metadata.empty))
      .add(StructField("vol", DoubleType, false, Metadata.empty))
      .add(StructField("hash", StringType, false, Metadata.empty))
      .add(StructField("confTime", LongType, false, Metadata.empty))
      .add(StructField("hourOfDay", LongType, false, Metadata.empty))
      .add(StructField("price", DoubleType, false, Metadata.empty))

    val schema2 = new StructType()
      .add(StructField("fee", DoubleType, false, Metadata.empty))
      .add(StructField("size", IntegerType, false, Metadata.empty))
      
}