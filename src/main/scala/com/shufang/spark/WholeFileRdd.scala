package com.shufang.spark

import com.shufang.utils.ScUtil
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD

/**
 * textFile() ：按行进行读取，可以使用正则匹配文件，可以一次性读取多个文件
 * wholeTextFiles(file)：按文件进行读取，返回一个Rdd[(k,v)]
 * wholeTextFiles(path): 按照目录中的文件进行读取
 */
object WholeFileRdd {
  def main(args: Array[String]): Unit = {

    val sc: SparkContext = ScUtil.getSc

    // TODO 1
    val rdd: RDD[(String, String)] = sc.wholeTextFiles("files/wordcount.txt")
    rdd.collect().foreach(println(_))

    println("=======")
    // TODO 2
    val rdd1: RDD[(String, String)] = sc.wholeTextFiles("files/")
    rdd1.collect().foreach(println)
    println("=======")

    // TODO 3
    val rdd2: RDD[String] = sc.textFile("files/*.txt")
    rdd2.collect().foreach(println)
    println("=======")

    sc.stop()
  }
}
