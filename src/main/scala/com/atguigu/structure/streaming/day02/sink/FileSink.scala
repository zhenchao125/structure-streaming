package com.atguigu.structure.streaming.day02.sink

import org.apache.spark.sql.{DataFrame, SparkSession}

/**
  * Author lzc
  * Date 2019-09-25 15:07
  */
object FileSink {
    def main(args: Array[String]): Unit = {
        val spark: SparkSession = SparkSession
            .builder()
            .master("local[*]")
            .appName("SteamSteamJoint")
            .getOrCreate()
        import spark.implicits._
        
        
        val nameSexStream: DataFrame = spark.readStream
            .format("socket")
            .option("host", "hadoop201")
            .option("port", 10000)
            .load
            .as[String]
            .toDF("value")
        
    }
}
