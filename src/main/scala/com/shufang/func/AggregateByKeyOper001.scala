package com.shufang.func

import com.shufang.utils.ScUtil
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD

/**
 * 将RDD中的数据按照key进行聚合
 * TODO 聚合分为分区内聚合、分区间聚合
 */
object AggregateByKeyOper001 {
  def main(args: Array[String]): Unit = {
    val sc: SparkContext = ScUtil.getSc

    val rdd: RDD[(String, Int)] = sc.makeRDD(List(
      ("a", 1), ("a", 2), ("b", 2), ("a", 3)
    ), 2)


    // TODO 初始值的类型与RDD返回值的value的类型是一样的
    val value: RDD[(String, (Int, Int))] = rdd.aggregateByKey((0, 0))(
      (tuple, v) => (tuple._1 + 1, tuple._2 + v),
      (tuple1, tuple2) => (tuple1._1 + tuple2._1, tuple1._2 + tuple2._2)
    )
    // 计算相同key的平均值
    val avgRDD: RDD[(String, Int)] = value.map {
      case (key, tuple) => (key, tuple._2 / tuple._1)
    }


    val rdd1: RDD[(String, Int)] = rdd.aggregateByKey(0)(
      _ + _,
      _ + _
    )
    //(b,2),(a,2)
    println(avgRDD.collect().mkString(","))

    sc.stop()
  }
}
