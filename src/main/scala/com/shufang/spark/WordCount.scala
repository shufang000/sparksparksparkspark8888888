package com.shufang.spark

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.rdd.RDD

object WordCount {
  def main(args: Array[String]): Unit = {

    // 1 获取上下文环境
    val conf: SparkConf = new SparkConf().setMaster("local[1]").setAppName("wordcount")
    val sc = new SparkContext(conf)

    // 2 从文件读取RDD
    val path: String = "D:/idea_projects/spark-rewatch-20210821/files/wordcount.txt"
    val sourceRdd: RDD[String] = sc.textFile(path)
    val rdd1: RDD[String] = sourceRdd.flatMap(_.split("\\s+"))
    val value: RDD[(String, Int)] = rdd1.map((_, 1))
    val wordCountRDD: RDD[(String, Int)] = value.reduceByKey(_ + _)
    wordCountRDD.collect().foreach(println(_))

    sc.stop()

  }
}
