package com.shufang.action

import com.shufang.utils.ScUtil
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD

import java.io

object CountByKeyDemo001 {
  def main(args: Array[String]): Unit = {
    val sc: SparkContext = ScUtil.getSc

    val rdd: RDD[Int] = sc.parallelize(1 to 10)
    println(rdd.countByValue())


    val rdd2: RDD[(String, Int)] = sc.makeRDD(List(("a", 1), ("b", 2),("a", 1)))

    println(rdd2.countByKey())
    sc.stop()
  }
}
