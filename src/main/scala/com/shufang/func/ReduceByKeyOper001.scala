package com.shufang.func

import com.shufang.utils.ScUtil
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD

/**
 * 将RDD中的数据按照key进行聚合
 * TODO 聚合分为分区内聚合、分区间聚合
 */
object ReduceByKeyOper001 {
  def main(args: Array[String]): Unit = {
    val sc: SparkContext = ScUtil.getSc

    val rdd: RDD[(String, Int)] = sc.makeRDD(List(
      ("a", 1), ("a", 2), ("b", 2), ("a", 3)
    ), 2)


    // TODO 1 reduceByKey()
    val rdd1: RDD[(String, Int)] = rdd.reduceByKey(_ + _)
    println(rdd1.collect().mkString(","))

    // TODO 2 groupByKey()
    val rdd2: RDD[(String, Int)] = rdd.groupByKey().map {
      case (key, iter) => (key, iter.reduce(_ + _))
    }
    println(rdd2.collect().mkString(","))

    // TODO 3 aggregateByKey()
    val rdd3: RDD[(String, Int)] = rdd.aggregateByKey(-1)(
      (zero, val1) => math.max(zero, val1), // 分区内的计算，预计算
      (a, b) => a + b // 分区间的计算
    )
    println(rdd3.collect().mkString(","))


    sc.stop()
  }
}
