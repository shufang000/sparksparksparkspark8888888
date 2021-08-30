package com.shufang.wc

import com.shufang.utils.ScUtil
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD

import scala.collection.mutable

object WordCountDemo001 {
  def main(args: Array[String]): Unit = {
    val sc: SparkContext = ScUtil.getSc

    val rdd: RDD[String] = sc.parallelize(List("hello world", "hello you"))

    // TODO 1
    val wordCount1: RDD[(String, Int)] = rdd.flatMap(_.split(" "))
      .groupBy(word => word)
      .map { case (s, iter) => (s, iter.size) }
    // TODO 2
    val wordCount2: RDD[(String, Int)] = rdd.flatMap(_.split(" "))
      .map((_, 1))
      .groupByKey()
      .mapValues(_.size)
    // TODO 3
    val wordCount3: RDD[(String, Int)] = rdd.flatMap(_.split(" "))
      .map((_, 1))
      .reduceByKey(_ + _)

    // TODO 4
    val wordCount4: RDD[(String, Int)] = rdd.flatMap(_.split(" "))
      .map((_, 1))
      .foldByKey(0)(_ + _)

    // TODO 5
    val wordCount5: RDD[(String, Int)] = rdd.flatMap(_.split(" "))
      .map((_, 1))
      .aggregateByKey(0)(_ + _, _ + _)


    // TODO 6
    val wordCount6: RDD[(String, Int)] = rdd.flatMap(_.split(" "))
      .map((_, 1))
      .combineByKey(
        v => v,
        (x: Int, v) => x + v,
        (x: Int, y: Int) => x + y
      )

    // TODO 7
    val stringToLong: collection.Map[String, Long] = rdd.flatMap(_.split(" "))
      .map((_, 1)).countByKey()

    // TODO 8
    val stringToLong2: collection.Map[String, Long] = rdd.flatMap(_.split(" "))
      .countByValue()

    // TODO 9
    val stringToInt3: mutable.Map[String, Int] = rdd.flatMap(_.split(" "))
      .map(
        word => {
          // 使用可变集合
          mutable.Map[String, Int](word -> 1)
        }
      ).reduce(
      (map1, map2) => {
        map2.foreach {
          case (s, c) => {
            val newCount: Int = map1.getOrElse(s, 0) + c
            map1.update(s, newCount)
          }
        }
        map1
      }
    )

    // TODO 10
    val stringToInt4: mutable.Map[String, Int] = rdd.flatMap(_.split(" "))
      .map(
        word => {
          // 使用可变集合
          mutable.Map[String, Int](word -> 1)
        }
      ).fold(mutable.Map[String,Int]())(
      (map1, map2) => {
        map2.foreach {
          case (s, c) => {
            val newCount: Int = map1.getOrElse(s, 0) + c
            map1.update(s, newCount)
          }
        }
        map1
      }
    )


    // TODO 11
    val stringToInt5: mutable.Map[String, Int] = rdd.flatMap(_.split(" "))
      .map(
        word => {
          // 使用可变集合
          mutable.Map[String, Int](word -> 1)
        }
      ).aggregate(mutable.Map[String,Int]())(
      (map1, map2) => {
        map2.foreach {
          case (s, c) => {
            val newCount: Int = map1.getOrElse(s, 0) + c
            map1.update(s, newCount)
          }
        }
        map1
      },
      (map1, map2) => {
        map2.foreach {
          case (s, c) => {
            val newCount: Int = map1.getOrElse(s, 0) + c
            map1.update(s, newCount)
          }
        }
        map1
      }
    )



    println(wordCount1.collect().mkString("-"))
    println(wordCount2.collect().mkString("-"))
    println(wordCount3.collect().mkString("-"))
    println(wordCount4.collect().mkString("-"))
    println(wordCount5.collect().mkString("-"))
    println(wordCount6.collect().mkString("-"))
    println(stringToLong.mkString("-"))
    println(stringToLong2.mkString("-"))
    println(stringToInt3.mkString("-"))
    println(stringToInt4.mkString("-"))
    println(stringToInt5.mkString("-"))


    sc.stop()
  }
}
