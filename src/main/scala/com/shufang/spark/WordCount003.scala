package com.shufang.spark

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object WordCount003 {
  def main(args: Array[String]): Unit = {

    val conf: SparkConf = new SparkConf().setAppName("wc").setMaster("local[*]")
    val sc = new SparkContext(conf)

    val rdd: RDD[String] = sc.parallelize(Seq("a", "a", "a,a", "b,b"))


    println(rdd.getNumPartitions)

    val wordCountRdd: RDD[(String, Int)] = rdd.flatMap(_.split(","))
      .groupBy(word => word)
      .map {
        case (word, list) =>
          (word, list.size)
      }

    wordCountRdd.collect().foreach(println)
    sc.stop()
  }
}
