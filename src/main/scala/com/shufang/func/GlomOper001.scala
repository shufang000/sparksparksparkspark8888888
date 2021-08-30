package com.shufang.func

import com.shufang.utils.ScUtil
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD

/**
 * 将一个分区中的数据变成一个数组，
 */
object GlomOper001 {
  def main(args: Array[String]): Unit = {
    val sc: SparkContext = ScUtil.getSc

    val rdd: RDD[Int] = sc.parallelize(1 to 10)

    val value: RDD[Array[Int]] = rdd.glom()


    println(value.getNumPartitions)

    value.collect().foreach(arr => println(arr.mkString(",")))

    sc.stop()
  }
}
