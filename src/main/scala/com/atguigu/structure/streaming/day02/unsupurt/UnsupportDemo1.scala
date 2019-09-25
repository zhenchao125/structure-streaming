package com.atguigu.structure.streaming.day02.unsupurt

import java.sql.Timestamp

import org.apache.spark.sql.{DataFrame, SparkSession}

/**
  * Author lzc
  * Date 2019-09-25 14:34
  */
object UnsupportDemo1 {
    def main(args: Array[String]): Unit = {
        val spark: SparkSession = SparkSession
            .builder()
            .master("local[*]")
            .appName("SteamSteamJoint")
            .getOrCreate()
        import spark.implicits._
    
        // 第 1 个 stream
        val nameSexStream: DataFrame = spark.readStream
            .format("socket")
            .option("host", "hadoop201")
            .option("port", 10000)
            .load
            .as[String]
            .toDF("v")
        
        nameSexStream.createOrReplaceTempView("user")
        spark.sql(
            """
              |select
              | *
              |from user
              |limit 1
            """.stripMargin)
            .writeStream
            .outputMode("append")
            .format("console")
            .start
            .awaitTermination()
        
    }
}
