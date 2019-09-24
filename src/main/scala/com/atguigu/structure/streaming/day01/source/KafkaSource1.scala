package com.atguigu.structure.streaming.day01.source

import org.apache.spark.sql.SparkSession

/**
  * Author lzc
  * Date 2019-09-24 15:14
  */
object KafkaSource1 {
    def main(args: Array[String]): Unit = {
        val spark: SparkSession = SparkSession
            .builder()
            .master("local[*]")
            .appName("KafkaSource")
            .getOrCreate()
        import spark.implicits._
        
        val df = spark.read
            .format("kafka")
            .option("kafka.bootstrap.servers", "hadoop201:9092,hadoop202:9092,hadoop203:9092")
            .option("subscribe", "topic1")
            .option("startingOffsets", """{"topic1":{"0":12}}""")
            .option("endingOffsets", "latest")
            .load
            //            .select("value", "key")
            .selectExpr("cast(value as string)")
            .as[String]
            .flatMap(_.split(" "))
            .groupBy("value")
            .count()
        
        // 批处理的方式, 只需要执行一次
        df.write
            .format("console")
            .option("truncate", false)
            .save()
        
    }
}
