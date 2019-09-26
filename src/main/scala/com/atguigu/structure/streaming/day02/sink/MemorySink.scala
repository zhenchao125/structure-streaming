package com.atguigu.structure.streaming.day02.sink

import java.util.{Timer, TimerTask}

import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.streaming.StreamingQuery

/**
  * Author lzc
  * Date 2019-09-25 15:38
  */
object MemorySink {
    def main(args: Array[String]): Unit = {
        val spark: SparkSession = SparkSession
            .builder()
            .master("local[2]")
            .appName("MemorySink")
            .getOrCreate()
        import spark.implicits._
    
        val lines: DataFrame = spark.readStream
            .format("socket") // 设置数据源
            .option("host", "hadoop201")
            .option("port", 10000)
            .load
    
        val words: DataFrame = lines.as[String]
            .flatMap(_.split("\\W+"))
            .groupBy("value")
            .count()
    
        val query: StreamingQuery = words.writeStream
            .outputMode("complete")
            .format("memory") // memory sink
            .queryName("word_count") // 内存临时表名
            .start
        
        val timer = new Timer
        val task = new TimerTask {
            override def run(): Unit = {
                spark.sql("select * from word_count").show
            }
        }
        timer.scheduleAtFixedRate(task, 0, 2000)
    
        query.awaitTermination()
    
    }
}
