package com.atguigu.structure.streaming.day01.source

import org.apache.spark.sql.SparkSession

/**
  * Author lzc
  * Date 2019-09-24 15:14
  */
object KafkaSource {
    def main(args: Array[String]): Unit = {
        val spark: SparkSession = SparkSession
            .builder()
            .master("local[*]")
            .appName("KafkaSource")
            .getOrCreate()
        import spark.implicits._
        
        val df = spark.readStream
            .format("kafka")
            .option("kafka.bootstrap.servers", "hadoop201:9092,hadoop202:9092,hadoop203:9092")
            .option("subscribe", "topic1")
            .load
            //            .select("value", "key")
            .selectExpr("cast(value as string)")
            .as[String]
            .flatMap(_.split(" "))
            .groupBy("value")
            .count()
        
        
        df.writeStream
            .format("console")
            .outputMode("update")
            .option("truncate", false)
            .start
            .awaitTermination()
        
    }
}
