package com.atguigu.structure.streaming.day02.window

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.streaming.Trigger

/**
  * Author lzc
  * Date 2019-09-24 21:47
  */
object Window1 {
    def main(args: Array[String]): Unit = {
        val spark: SparkSession = SparkSession
            .builder()
            .master("local[*]")
            .appName("Window1")
            .getOrCreate()
        import org.apache.spark.sql.functions._
        import spark.implicits._
        spark.readStream
            .format("socket")
            .option("host", "hadoop201")
            .option("port", 9999)
            .load()
            .as[String]
            .map(line => {
                val split: Array[String] = line.split(",")
                (split(0), split(1))
            })
            .toDF("ts", "word")
            .groupBy(
                window($"ts", "10 minutes", "3 minutes"),
                $"word"
            )
            .count()
            .sort("window")
            .writeStream
            .format("console")
            .outputMode("complete")
            .trigger(Trigger.ProcessingTime(2000))
            .option("truncate", false)
            .start
            .awaitTermination()
    }
}
