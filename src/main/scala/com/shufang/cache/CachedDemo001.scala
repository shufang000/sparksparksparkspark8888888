package com.shufang.cache

import com.shufang.utils.ScUtil
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD
import org.apache.spark.storage.StorageLevel

object CachedDemo001 {
  def main(args: Array[String]): Unit = {
    val sc: SparkContext = ScUtil.getSc

    sc.setCheckpointDir("cp")

    val value: RDD[Int] = sc.makeRDD(1 to 2,1)

    val rdd1: RDD[Int] = value.map {
      int => {
        println("@@@@")
        int * 2
      }
    }

    println(rdd1.toDebugString)

    //rdd1.cache()
    //rdd1.persist(StorageLevel.DISK_ONLY)
    rdd1.cache()
    rdd1.checkpoint()


    println("==================")
    rdd1.collect()
    rdd1.collect()
    println(rdd1.toDebugString)




    sc.stop()
  }
}
