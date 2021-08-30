package com.shufang.func

import com.shufang.utils.ScUtil
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD

/**
 * 将RDD中的数据进行去重
 */
object DistinctOper001 {
  def main(args: Array[String]): Unit = {
    val sc: SparkContext = ScUtil.getSc

    val rdd: RDD[Int] = sc.parallelize(List(1,2,3,4,1,2,3,4,1,2,3,4))

    // TODO 与Scala集合distinct去重不同，scala本身集合使用额HashSet进行的去重
    val rdd1: RDD[Int] = rdd.distinct()

    // TODO map(x => (x, null)).reduceByKey((x, _) => x, numPartitions).map(_._1)，这是distinct算子的逻辑
    val distinctRDD1: RDD[Int] = rdd.map(x => (x, null)).reduceByKey((x, _) => x).map(_._1)


    rdd1.collect().foreach(println(_))
    distinctRDD1.collect().foreach(println(_))

    sc.stop()
  }
}
