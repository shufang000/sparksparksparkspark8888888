package com.shufang.sparkstreaming

import org.apache.spark.SparkConf
import org.apache.spark.streaming.dstream.ReceiverInputDStream
import org.apache.spark.streaming.{Seconds, StreamingContext}

object StreamingQuickStartDemo001 {

  def main(args: Array[String]): Unit = {
    val conf: SparkConf = new SparkConf().setAppName("SparkStreaming").setMaster("local[*]")

    val ssc = new StreamingContext(conf, Seconds(3))


    val streaming1: ReceiverInputDStream[String] = ssc.socketTextStream("shufang101", 9999)




    streaming1.print()


    ssc.start() //开始采集数据并触发job

    ssc.awaitTermination()  //阻塞main线程
  }
}
