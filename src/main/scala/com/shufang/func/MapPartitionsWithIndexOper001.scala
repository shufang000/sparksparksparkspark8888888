package com.shufang.func

import com.shufang.utils.ScUtil
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD

object MapPartitionsWithIndexOper001 {
  def main(args: Array[String]): Unit = {
    val sc: SparkContext = ScUtil.getSc

    val rdd: RDD[Int] = sc.makeRDD(List(1, 2, 3, 4), 2)

    /* 取1号分区的所有数据
        val value: RDD[Int] = rdd.mapPartitionsWithIndex((parNum, iter) => {
          if (parNum == 1) {
            iter
          } else {
            Nil.iterator
          }
        })*/


    val rddWithIndex: RDD[(Int, Int)] = rdd.mapPartitionsWithIndex(
      (num, iter) => {
        iter.map {
          int =>
            if (num == 1)
              (num, int * 2)
            else
              (num, int)
        }
      }
    )


    rddWithIndex.collect().foreach(println(_))

    sc.stop()
  }
}
