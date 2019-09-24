package com.atguigu.structure.streaming.day01

import org.apache.spark.sql.streaming.{StreamingQuery, Trigger}
import org.apache.spark.sql.{DataFrame, SparkSession}

/**
  * Author lzc
  * Date 2019-09-24 10:40
  */
object WordCount1 {
    def main(args: Array[String]): Unit = {
        val spark: SparkSession = SparkSession
            .builder()
            .master("local[*]")
            .appName("WordCount1")
            .getOrCreate()
        import spark.implicits._
        // 1. 从载数据数据源加
        val lines: DataFrame = spark.readStream
            .format("socket")
            .option("host", "hadoop201")
            .option("port", 9999)
            .load
        
        
//        val wordCount = lines.as[String].flatMap(_.split(" ")).groupBy("value").count()
        
        lines.as[String].flatMap(_.split(" ")).createOrReplaceTempView("w")
        val wordCount = spark.sql(
            """
              |select
              | *
              |from w
            """.stripMargin)
        // 2. 输出
        val result: StreamingQuery = wordCount.writeStream
            .format("console")
            .outputMode("append")   // complete append update
            .trigger(Trigger.ProcessingTime("2 seconds"))
            .start
        
        result.awaitTermination()
        spark.stop()
    }
}
/*
compete
    全部输出, 必须有聚合
append
    追加模式.  只输出那些将来永远不可能再更新的数据
    没有聚合的时候, append和update一致
    有聚合的时候, 一定要有水印才能使用append
update
    只输出变化的部分
 */