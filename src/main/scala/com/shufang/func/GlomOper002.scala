package com.shufang.func

import com.shufang.utils.ScUtil
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD

/**
 * 将一个分区中的数据变成一个数组，
 */
object GlomOper002 {
  def main(args: Array[String]): Unit = {
    val sc: SparkContext = ScUtil.getSc

    val rdd: RDD[Int] = sc.parallelize(1 to 10)

    val value: RDD[Array[Int]] = rdd.glom()


    println(value.getNumPartitions)

    val value1: RDD[Int] = value.map(
      arr => {
        println(arr.max)
        arr.max
      }
    )

    println(value1.collect().sum)

    sc.stop()
  }
}
