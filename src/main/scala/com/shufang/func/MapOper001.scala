package com.shufang.func

import com.shufang.utils.ScUtil
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD

object MapOper001 {
  def main(args: Array[String]): Unit = {
    val sc: SparkContext = ScUtil.getSc

    val rdd: RDD[Int] = sc.parallelize(List(1, 2, 3, 4))

    /*val value: RDD[Int] = rdd.map((num: Int) => {
      2 * num
    })*/

    /*def mul(num:Int):Int = {
      num * 2
    }
    val value: RDD[Int] = rdd.map(mul)*/

    rdd.map(a => a*2)
    val value: RDD[Int] = rdd.map(_ * 2)

    value.collect().foreach(println)
    sc.stop()
  }
}
