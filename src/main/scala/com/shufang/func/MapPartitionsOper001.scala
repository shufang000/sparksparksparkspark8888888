package com.shufang.func

import com.shufang.utils.ScUtil
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD

object MapPartitionsOper001 {
  def main(args: Array[String]): Unit = {
    val sc: SparkContext = ScUtil.getSc

    val rdd: RDD[Int] = sc.makeRDD(List(1, 2, 3, 4),2)


    /**
     * 在内存较小，数据量比较大的时候可能会出现内存的溢出
     */
    val rdd1: RDD[Int] = rdd.mapPartitions(
      iter => {
        println(">>>")
        iter.map(_ * 2)
      }
    )


    val rdd2: RDD[Int] = rdd.mapPartitions(
      iter => {
        val max: Int = iter.max
        List(max).iterator
      }
    )

    rdd2.collect().foreach(println(_))

    sc.stop()
  }
}
