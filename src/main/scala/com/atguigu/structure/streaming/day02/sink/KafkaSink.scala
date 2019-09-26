package com.atguigu.structure.streaming.day02.sink

import org.apache.spark.sql.{DataFrame, SparkSession}

/**
  * Author lzc
  * Date 2019-09-25 15:07
  */
object KafkaSink {
    def main(args: Array[String]): Unit = {
        val spark: SparkSession = SparkSession
            .builder()
            .master("local[1]")
            .appName("Test")
            .getOrCreate()
        import spark.implicits._
        
        val lines: DataFrame = spark.readStream
            .format("socket") // 设置数据源
            .option("host", "hadoop201")
            .option("port", 10000)
            .load
        
        val words: DataFrame = lines.as[String].flatMap(line => {
            line.split("\\W+")
        }).toDF("value")
        
        words.writeStream
            .outputMode("append")
            .format("kafka") //  // 支持 "orc", "json", "csv"
            .option("kafka.bootstrap.servers", "hadoop201:9092,hadoop202:9092,hadoop203:9092")
            .option("topic", "ss0508")
            .option("checkpointLocation", "./ck3") // 必须指定 checkpoint 目录
            .start
            .awaitTermination()
        
        
    }
}
