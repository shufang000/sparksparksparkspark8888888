package com.shufang.func

import com.shufang.utils.ScUtil
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD

/**
 * 求出不同省份的广告点击的top3
 * (河北,List((A,3), (B,2), (C,1)))
 * (河南,List((A,4), (B,1)))
 *
 */
object CoGroupOper001 {
  def main(args: Array[String]): Unit = {
    val sc: SparkContext = ScUtil.getSc


    val sourceRDD: RDD[String] = sc.textFile("files/ads.txt")


    val rdd1: RDD[((String, String), Int)] = sourceRDD.map {
      case line => {
        val words: Array[String] = line.split("  ")
        ((words(0), words(1)), 1)
      }
    }

    val countRDD: RDD[((String, String), Int)] = rdd1.reduceByKey(_ + _)

    val rdd2: RDD[(String, (String, Int))] = countRDD.map {
      case (tuple, count) =>
        (tuple._1, (tuple._2, count))
    }


    val groupedRDD: RDD[(String, Iterable[(String, Int)])] = rdd2.groupByKey()


    val rdd3: RDD[(String, List[(String, Int)])] = groupedRDD.mapValues(
      // 倒序排序，取前三
      iter => iter.toList.sortBy(_._2)(Ordering.Int.reverse).take(3)
    )


    rdd3.foreach(println)

    sc.stop()
  }
}
