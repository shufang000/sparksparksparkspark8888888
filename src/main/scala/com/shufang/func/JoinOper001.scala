package com.shufang.func

import com.shufang.utils.ScUtil
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD

import java.io

/**
 * 将一个分区中的数据变成一个数组，
 */
object JoinOper001 {
  def main(args: Array[String]): Unit = {
    val sc: SparkContext = ScUtil.getSc


    val rdd1: RDD[(String, Int)] = sc.makeRDD(List(("a", 1), ("a", 2), ("b", 5)))
    val rdd2: RDD[(String, Int)] = sc.makeRDD(List(("a", 4), ("a",8), ("b", 33)))


/*    rdd1.join(rdd2).collect().foreach(println)
    rdd1.leftOuterJoin(rdd2).collect().foreach(println)
    rdd1.rightOuterJoin(rdd2).collect().foreach(println)*/

    val value: RDD[(String, (Iterable[Int], Iterable[Int]))] = rdd1.cogroup(rdd2)

    value.foreach(println(_))

    sc.stop()
  }
}
