package com.atguigu.structure.streaming.day02.sink

import java.util.Properties

import org.apache.spark.sql.streaming.{StreamingQuery, Trigger}
import org.apache.spark.sql.{DataFrame, SparkSession}

/**
  * Author lzc
  * Date 2019-09-25 15:48
  */
object ForeachBatchSink1 {
    def main(args: Array[String]): Unit = {
        val spark: SparkSession = SparkSession
            .builder()
            .master("local[2]")
            .appName("ForeachSink")
            .getOrCreate()
        import spark.implicits._
        
        val lines: DataFrame = spark.readStream
            .format("socket") // 设置数据源
            .option("host", "hadoop201")
            .option("port", 10000)
            .load
        
        val props = new Properties()
        props.setProperty("user", "root")
        props.setProperty("password", "aaa")
        
        val query: StreamingQuery = lines.writeStream
            .outputMode("update")
            .foreachBatch((df, batchId) => {
                val result = df.as[String].flatMap(_.split("\\W+")).groupBy("value").count()
                    
                    /*createOrReplaceTempView("user")
                val result = spark.sql("select value, count(*) from user group by value")
    */
                result.persist()
                result.write.mode("overwrite").jdbc("jdbc:mysql://hadoop201:3306/ss","word_count0508", props)
                result.write.mode("overwrite").json("./foreach0508")
                result.unpersist()
            })
//            .trigger(Trigger.ProcessingTime(0))
            .trigger(Trigger.Continuous(10))
            .start
        query.awaitTermination()
        
        
    }
}
