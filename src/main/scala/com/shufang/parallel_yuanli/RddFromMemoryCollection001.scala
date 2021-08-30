package com.shufang.parallel_yuanli

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
 * 不管在makeRDD 还是 parallel方法，都可以指定一个生成的RDD的分区个数，如果没有指定，那么分区的个数就会按照不同的运行环境的默认值来分配
 * scheduler.conf.getInt("spark.default.parallelism", totalCores)
 * 如果通过SparkConf指定了 spark.default.parallelism，那么RDD的分区的个数就为spark.default.parallelism
 * 否则就取当前环境master可以使用的最大的虚拟核数：
 */
object RddFromMemoryCollection001 {
  def main(args: Array[String]): Unit = {

    val conf: SparkConf = new SparkConf().setMaster("local[8]").setAppName("parellel")
      .set("spark.default.parallelism", "5")

    val sc: SparkContext = new SparkContext(conf)

    /*// numSlice代表生成Rdd的分区个数:2
    val rdd: RDD[Int] = sc.makeRDD(List(1, 2, 3, 4), 2)*/

    val rdd: RDD[Int] = sc.makeRDD(List(1, 2, 3, 4))

    rdd.saveAsTextFile("output")

    sc.stop()
  }
}
