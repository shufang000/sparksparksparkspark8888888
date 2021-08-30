package com.shufang.func

import com.shufang.utils.ScUtil
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD

/**
 * 将RDD中的数据进行去重
 */
object CoalesceOper001 {
  def main(args: Array[String]): Unit = {
    val sc: SparkContext = ScUtil.getSc

    val rdd: RDD[Int] = sc.parallelize(List(1,2,3,4,1,2,3,4,1,2,3,4),3)


    // TODO coalesce() 算子是不会将同一分区的数据进行打散然后进行重新分配的,如果需要打散再均衡，直接使用 shuffle参数 = true
    val value: RDD[Int] = rdd.coalesce(2,false)

    val value1: RDD[(Int, Int)] = value.mapPartitionsWithIndex(
      (index, iter) => {
        iter.map(
          (index, _)
        )
      }
    )

    value1.repartition(10)


    value1.foreach(println(_))

    sc.stop()
  }
}
