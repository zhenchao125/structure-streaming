package com.atguigu.structure.streaming.day02.joint

import java.sql.Timestamp

import org.apache.spark.sql.streaming.Trigger
import org.apache.spark.sql.{DataFrame, SparkSession}

/**
  * Author lzc
  * Date 2019/8/16 5:09 PM
  */
object SteamSteamJoint {
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
            .map(line => {
                val arr: Array[String] = line.split(",")
                (arr(0), arr(1), Timestamp.valueOf(arr(2)))
            }).toDF("name", "sex", "ts1")
            .withWatermark("ts1", "2 minutes")
        
        // 第 2 个 stream
        val nameAgeStream: DataFrame = spark.readStream
            .format("socket")
            .option("host", "hadoop201")
            .option("port", 20000)
            .load
            .as[String]
            .map(line => {
                val arr: Array[String] = line.split(",")
                (arr(0), arr(1).toInt, Timestamp.valueOf(arr(2)))
            }).toDF("name", "age", "ts2")
            .withWatermark("ts2", "1 minutes")
        
        // join 操作
        val joinResult: DataFrame = nameSexStream.join(nameAgeStream, "name")
        
        joinResult.writeStream
            .outputMode("append")
            .format("console")
            .trigger(Trigger.ProcessingTime(0))
            .start()
            .awaitTermination()
    }
}

