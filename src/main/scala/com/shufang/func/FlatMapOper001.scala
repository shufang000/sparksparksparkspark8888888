package com.shufang.func

import com.shufang.utils.ScUtil
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD

object FlatMapOper001 {
  def main(args: Array[String]): Unit = {


    val sc: SparkContext = ScUtil.getSc

    val value: RDD[List[Int]] = sc.parallelize(List(List(1, 2), List(3, 4)))
    val value1: RDD[Int] = value.flatMap(list => list)


    value1.collect()


    /**
     * 配合模式匹配将类型进行统一
     * match case
     */
    val rdd1: RDD[Any] = sc.parallelize(List(List(1, 2), 5, 6, List(3, 4)))

    val rdd2: RDD[Any] = rdd1.flatMap(
      data => {
        data match {
          case list: List[_] => list
          case dat => List(dat)
          case _ => Nil
        }
      }
    )

    rdd2.collect().foreach(println)
    sc.stop()
  }
}
