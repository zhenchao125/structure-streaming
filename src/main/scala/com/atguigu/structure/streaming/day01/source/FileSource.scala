package com.atguigu.structure.streaming.day01.source

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.streaming.Trigger
import org.apache.spark.sql.types.{IntegerType, StringType, StructField, StructType}

/**
  * Author lzc
  * Date 2019-09-24 14:14
  */
object FileSource {
    def main(args: Array[String]): Unit = {
        val spark: SparkSession = SparkSession
            .builder()
            .master("local[*]")
            .appName("FileSource")
            .getOrCreate()
        
        val userSchema = StructType(StructField("name", StringType) :: StructField("age", IntegerType) :: StructField("sex", StringType) :: Nil)
        val df = spark.readStream
            .format("csv")
            .schema(userSchema)
            .load("C:\\Users\\lzc\\Desktop\\ss")
            .groupBy("sex")
            .sum("age")
        
        
        //
        df.writeStream
            .format("console")
            .outputMode("update")
            .trigger(Trigger.ProcessingTime(1000))
            .start()
            .awaitTermination()
    }
}
