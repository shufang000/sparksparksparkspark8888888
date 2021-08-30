package com.shufang.spark

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object WordCount002 {
  def main(args: Array[String]): Unit = {
    // 1 获取上下文环境
    val conf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("wordcount")
    val sc = new SparkContext(conf)

    // 2 从文件读取RDD
    val path: String = "D:/idea_projects/spark-rewatch-20210821/files/wordcount.txt"
    val rdd1: RDD[String] = sc.textFile(path)
      .flatMap(_.split("\\s+"))
    println(rdd1.getNumPartitions) //2  :math.min(defaultParallelism, 2)


    val rdd2: RDD[(String, Iterable[String])] = rdd1.groupBy(word => word)

    val wordCount: RDD[(String, Int)] = rdd2.map {
      case (word, list) =>
        val count: Int = list.size
        (word, count)
    }


    wordCount.foreach(println)

    sc.stop()

  }
}
