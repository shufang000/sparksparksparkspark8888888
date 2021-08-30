package com.shufang.func

import com.shufang.utils.ScUtil
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD

/**
 * 将RDD中的数据按照key进行聚合
 * TODO 聚合分为分区内聚合、分区间聚合
 */
object CombineByKeyOper001 {
  def main(args: Array[String]): Unit = {
    val sc: SparkContext = ScUtil.getSc

    val rdd: RDD[(String, Int)] = sc.makeRDD(List(
      ("a", 1), ("a", 2), ("b", 2), ("a", 3)
    ), 2)


    /**
     * 第一个是将RDD中的第一个元素的value类型进行转换
     * 第二个参数是分区内的聚合
     * 第三个参数是分区间的聚合
     */
    val rdd1: RDD[(String, (Int, Int))] = rdd.combineByKey(
      intVal => (1, intVal),
      (t: (Int, Int), v) => (t._1 + 1, t._2 + v),
      (t1: (Int, Int), t2: (Int, Int)) => (t1._1 + t2._1, t1._2 + t2._2)
    )
    val avgRDD: RDD[(String, Int)] = rdd1.map {
      case (key, t) => (key, t._2 / t._1)
    }


    val rdd3: RDD[(String, Int)] = rdd.combineByKey(
      v => v,
      (x: Int, v) => x + v,
      (x: Int, y: Int) => x + y
    )

    avgRDD.collect().foreach(println)

    sc.stop()
  }
}
