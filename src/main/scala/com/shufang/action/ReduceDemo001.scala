package com.shufang.action

import com.shufang.utils.ScUtil
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD

object ReduceDemo001 {
  def main(args: Array[String]): Unit = {
    val sc: SparkContext = ScUtil.getSc

    val rdd: RDD[Int] = sc.makeRDD(1 to 4,3)

    /**
     * TODO 与ByKey聚合算子相比，ByKey聚合算子的zerovalue只会在分区内参与计算，不会在分区间进行计算
     *      而action的算子的zerovalue如下面的10，既会在分区内计算 10*3 + 10 ，同时也会在份区间计算时参与计算10
     *       => 50
     */
    println(rdd.reduce(_ + _))
    println(rdd.fold(10)(_ + _)) //40
    println(rdd.count())
    println(rdd.aggregate(10)(_ + _, _ + _)) //40
    println(rdd.first())

    val ints: Array[Int] = rdd.takeOrdered(1)
    println(ints(0))





    sc.stop()
  }
}
