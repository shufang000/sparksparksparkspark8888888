package com.shufang.depend

import com.shufang.utils.ScUtil
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD

object RDDDependencyDemo001 {

  def main(args: Array[String]): Unit = {

    val sc: SparkContext = ScUtil.getSc

    val rdd: RDD[String] = sc.textFile("files/ads.txt")


    /**
     * (2) files/ads.txt MapPartitionsRDD[1] at textFile at RDDDependencyDemo001.scala:13 []
     * |  files/ads.txt HadoopRDD[0] at textFile at RDDDependencyDemo001.scala:13 []
     */
    println(rdd.toDebugString)
    println(rdd.dependencies)  //OneToOneDependency
    val rdd1: RDD[(String, Int)] = rdd.map((_, 1)).reduceByKey(_+_)

    /**
     * (2) MapPartitionsRDD[2] at map at RDDDependencyDemo001.scala:22 []
     * |  files/ads.txt MapPartitionsRDD[1] at textFile at RDDDependencyDemo001.scala:13 []
     * |  files/ads.txt HadoopRDD[0] at textFile at RDDDependencyDemo001.scala:13 []
     */
    println(rdd1.toDebugString)
    println(rdd1.dependencies)  //ShuffleDependency


    rdd.collect()
    sc.stop()

  }

}
