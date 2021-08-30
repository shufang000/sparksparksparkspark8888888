package com.shufang.func

import com.shufang.utils.ScUtil
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD

/**
 * 将RDD中的数据进行去重
 */
object IntersaOper001 {
  def main(args: Array[String]): Unit = {
    val sc: SparkContext = ScUtil.getSc
    sc.getConf.setMaster("local[1]")
    val rdd1: RDD[Int] = sc.makeRDD(1 to 4)
    val rdd2: RDD[Int] = sc.makeRDD(3 to 6)

    // TODO 并集
    val value: RDD[Int] = rdd1.intersection(rdd2)
    // TODO 交集
    val value1: RDD[Int] = rdd1.union(rdd2)
    // TODO 差集
    val value2: RDD[Int] = rdd1.subtract(rdd2)
    // TODO 拉链
    val rdd3: RDD[(Int, Int)] = rdd1.zip(rdd2)

    println(value.collect().mkString(","))
    println(value1.collect().mkString(","))
    println(value2.collect().mkString(","))
    println(rdd3.collect().mkString(","))

    sc.stop()
  }
}
