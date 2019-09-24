package com.atguigu.structure.streaming.day01.option

import org.apache.spark.sql.types.{LongType, StringType, StructType}
import org.apache.spark.sql.{DataFrame, SparkSession}

/**
  * Author lzc
  * Date 2019-09-24 16:27
  */
object UnTypeOpt {
    def main(args: Array[String]): Unit = {
        val spark: SparkSession = SparkSession
            .builder()
            .master("local[*]")
            .appName("BasicOperation")
            .getOrCreate()
        val peopleSchema: StructType = new StructType()
            .add("name", StringType)
            .add("age", LongType)
            .add("sex", StringType)
        val peopleDF: DataFrame = spark.readStream
            .schema(peopleSchema)
            .json("C:\\Users\\lzc\\Desktop\\ss\\json")  // 等价于: format("json").load(path)
        
        
        val df: DataFrame = peopleDF.select("name", "age", "sex").where("age > 20").groupBy("sex").sum("age") // 弱类型 api
        df.writeStream
            .outputMode("complete")
            .format("console")
            .start
            .awaitTermination()
    }
    

}
