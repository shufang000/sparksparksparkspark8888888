package com.shufang.func

import com.shufang.utils.ScUtil
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD

/**
 * 将一个分区中的数据变成一个数组，
 */
object SampleOper001 {
  def main(args: Array[String]): Unit = {
    val sc: SparkContext = ScUtil.getSc

    val rdd: RDD[Int] = sc.parallelize(List(1,2,3,4,5,6,7,8,9,10))


    val sample: RDD[Int] = rdd.sample(
      false, //抽取之后会不会被放回再次被抽取
      0.4, //这是每个数据被抽取的概率
      1 //这是一个种子
    )

    val sample1: RDD[Int] = rdd.sample(
      false, //抽取之后会不会被放回再次被抽取
      0.3, //这是每个数据被抽取的概率
    )
    val sample2: RDD[Int] = rdd.sample(
      true, //抽取之后会不会被放回再次被抽取
      3, //这是每个数据被抽取的概率
    )

    println(sample.collect().mkString(","))
    println(sample1.collect().mkString(","))
    println(sample2.collect().mkString(","))

    sc.stop()
  }
}
