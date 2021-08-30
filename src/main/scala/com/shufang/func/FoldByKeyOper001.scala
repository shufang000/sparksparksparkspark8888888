package com.shufang.func

import com.shufang.utils.ScUtil
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD

/**
 * 将RDD中的数据按照key进行聚合
 * TODO 聚合分为分区内聚合、分区间聚合
 */
object FoldByKeyOper001 {
  def main(args: Array[String]): Unit = {
    val sc: SparkContext = ScUtil.getSc

    val rdd: RDD[(String, Int)] = sc.makeRDD(List(
      ("a", 1), ("a", 2), ("b", 2), ("a", 3)
    ), 2)

    //TODO foldByKey是reduceByKey的简化版，默认是分区内聚合和分区间的聚合操作是一样的
    val rdd1: RDD[(String, Int)] = rdd.foldByKey(0)(_ + _)

    println(rdd1.collect().mkString(","))

    sc.stop()
  }
}
