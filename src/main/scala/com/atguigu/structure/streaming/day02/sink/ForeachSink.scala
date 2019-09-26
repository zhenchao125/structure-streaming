package com.atguigu.structure.streaming.day02.sink

import java.sql.{Connection, DriverManager, PreparedStatement}

import org.apache.spark.sql.{DataFrame, ForeachWriter, Row, SparkSession}
import org.apache.spark.sql.streaming.StreamingQuery

/**
  * Author lzc
  * Date 2019-09-25 15:48
  */
object ForeachSink {
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
    
        val wordCount: DataFrame = lines.as[String]
            .flatMap(_.split("\\W+"))
            .groupBy("value")
            .count()  // value count
    
        val query: StreamingQuery = wordCount.writeStream
            .outputMode("update")
            .foreach(new ForeachWriter[Row] {
                val sql = "insert into word_count0508 values(?, ?) on duplicate key update word=?, count=?";
                var conn: Connection = null
                // open 一般用于打开链接  false表示跳过该区的数据
                override def open(partitionId: Long, epochId: Long): Boolean = {
                    Class.forName("com.mysql.jdbc.Driver")
                    conn = DriverManager.getConnection("jdbc:mysql://hadoop201:3306/ss", "root", "aaa")
                    
                    conn != null && !conn.isClosed
                }
                
                // 把数据写入到连接中
                override def process(value: Row): Unit = {
    
                    val ps: PreparedStatement = conn.prepareStatement(sql)
                    ps.setString(1, value.getString(0))
                    ps.setLong(2, value.getLong(1))
    
                    ps.setString(3, value.getString(0))
                    ps.setLong(4, value.getLong(1))
                    ps.execute()
                    ps.close()
                }
                
                // 关闭连接
                override def close(errorOrNull: Throwable): Unit = {
                    if(conn != null && !conn.isClosed) conn.close()
                }
            })
        
            .start
            query.awaitTermination()
        
    
    }
}
