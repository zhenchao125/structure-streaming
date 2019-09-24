package com.atguigu.structure.streaming.day01.option

import org.apache.spark.sql.types.{LongType, StringType, StructType}
import org.apache.spark.sql.{DataFrame, SparkSession}

/**
  * Author lzc
  * Date 2019-09-24 16:27
  */
object TypeOpt {
    def main(args: Array[String]): Unit = {
        val spark: SparkSession = SparkSession
            .builder()
            .master("local[*]")
            .appName("BasicOperation")
            .getOrCreate()
        import spark.implicits._
        val peopleSchema: StructType = new StructType()
            .add("name", StringType)
            .add("age", LongType)
            .add("sex", StringType)
        val peopleDF: DataFrame = spark.readStream
            .schema(peopleSchema)
            .json("C:\\Users\\lzc\\Desktop\\ss\\json")  // 等价于: format("json").load(path)
    
        val ds = peopleDF.as[People].filter(_.age > 20).map(_.name)
        
        ds.writeStream
            .outputMode("update")
            .format("console")
            .start
            .awaitTermination()
    }
    

}

case class People(name: String, age: Long, sex: String)
