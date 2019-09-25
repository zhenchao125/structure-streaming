package com.atguigu.structure.streaming.day02.joint

import org.apache.spark.sql.{DataFrame, SparkSession}

/**
  * Author lzc
  * Date 2019-09-25 14:00
  */
object SteamingStatic {
    def main(args: Array[String]): Unit = {
        val spark: SparkSession = SparkSession
            .builder()
            .master("local[*]")
            .appName("SteamingStatic")
            .getOrCreate()
        import spark.implicits._
        
        // 得到静态的df
        val arr = Array(("lisi", 20), ("zs", 10), ("ww", 15))
        val staticDF = spark.sparkContext.parallelize(arr).toDF("name", "age")
        // 动态df
        val steamingDF = spark.readStream
            .format("socket")
            .option("host", "hadoop201")
            .option("port", 9999)
            .load
            .as[String]
            .map(line => {
               val splits = line.split(",")
                (splits(0), splits(1))
            })
            .toDF("name", "sex")
        
        // 内连接
        val joinedDF: DataFrame = steamingDF.join(staticDF, Seq("name"))
        
        joinedDF.writeStream
            .format("console")
            .outputMode("update")
            .start()
            .awaitTermination()
    }
}
