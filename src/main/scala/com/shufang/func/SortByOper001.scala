package com.shufang.func

import com.shufang.utils.ScUtil
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD

import scala.collection.mutable.ArrayBuffer

/**
 * 将RDD中的数据进行排序
 */
object SortByOper001 {
  def main(args: Array[String]): Unit = {

    val buffer: ArrayBuffer[Int] = new ArrayBuffer()
    buffer.append(1,2,3,4,5,6,10,1)


    val ints1: ArrayBuffer[Int] = buffer.sortWith((a, b) => a > b)
    println(ints1.mkString(","))

    val ints: ArrayBuffer[Int] = buffer.sortBy(num => num)
    println(ints.mkString(","))

    println(buffer.mkString(","))

    val sorted: ArrayBuffer[Int] = buffer.sorted
    println(sorted.mkString(","))



    val sc: SparkContext = ScUtil.getSc

    val rdd: RDD[Int] = sc.parallelize(List(1,2,3,4,1,2,3,4,1,2,3,4),1)



    val value: RDD[Int] = rdd.sortBy(num => num,false)


    value.foreach(println(_))

    sc.stop()
  }
}
