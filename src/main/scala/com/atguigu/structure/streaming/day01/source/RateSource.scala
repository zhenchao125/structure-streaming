package com.atguigu.structure.streaming.day01.source

import org.apache.spark.sql.SparkSession

/**
  * Author lzc
  * Date 2019-09-24 16:17
  */
object RateSource {
    def main(args: Array[String]): Unit = {
        val spark: SparkSession = SparkSession
            .builder()
            .master("local[*]")
            .appName("RateSource")
            .getOrCreate()
        
        val df = spark.readStream
            .format("rate")
            .option("rowsPerSecond", 1000)
            .option("rampUpTime", 1)
            .option("numPartitions", 3)
            .load
        
        df.writeStream
            .format("console")
            .outputMode("update")
            .option("truncate", false)
            .start
            .awaitTermination()
    }
}
