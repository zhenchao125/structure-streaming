package com.atguigu.structure.streaming.day01.window

import java.sql.Timestamp

import org.apache.spark.sql.SparkSession

/**
  * Author lzc
  * Date 2019-09-24 16:50
  */
object window1 {
    def main(args: Array[String]): Unit = {
        val spark: SparkSession = SparkSession
            .builder()
            .master("local[*]")
            .appName("window1")
            .getOrCreate()
        import spark.implicits._
        
        // 导入spark提供的全局的函数
        import org.apache.spark.sql.functions._
        val lines = spark.readStream
            .format("socket") // 设置数据源
            .option("host", "hadoop201")
            .option("port", 9999)
            .option("includeTimestamp", true) // 给产生的数据自动添加时间戳
            .load
            .as[(String, Timestamp)]
            .flatMap {
                case (words, ts) => words.split("\\W+").map((_, ts))
            }
            .toDF("word", "ts")
            .groupBy(
                window($"ts", "4 minutes", "2 minutes"),
                $"word")
            .count()
        lines.writeStream
            .format("console")
            .outputMode("update")
            .option("truncate", false)
            .start()
            .awaitTermination()
        
        
    }
}
