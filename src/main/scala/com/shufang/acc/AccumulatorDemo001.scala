package com.shufang.acc

import com.shufang.utils.ScUtil
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD
import org.apache.spark.util.{AccumulatorV2, LongAccumulator}

import scala.collection.mutable

object AccumulatorDemo001 {
  def main(args: Array[String]): Unit = {
    val sc: SparkContext = ScUtil.getSc

    // 通常累加器需要通过action算子触发，多个action算子可能会造成重复累加
    /*val longAcc: LongAccumulator = sc.longAccumulator("longAcc")

    sc.makeRDD(1 to 5)
      .foreach{
        longAcc.add(1L)
        println(_)
      }


    println(longAcc.value)*/
    val myAcc = new AccumulatorDemo001
    sc.register(myAcc,"udfAcc");

    val rdd: RDD[String] = sc.makeRDD(List("a", "a", "b", "a", "c"))

    rdd.foreach {
      case word =>{
        myAcc.add(word)
        println(word)

      }
    }


    println(myAcc.value)
    sc.stop()
  }
}


/**
 * 自定义累加器,实现wordCount
 */
class AccumulatorDemo001 extends AccumulatorV2[String,mutable.Map[String,Int]]{

  private var map: mutable.Map[String, Int] = mutable.Map[String, Int]()
  //判断是否为初始化的累加器，如果map为空，表示为初始化
  override def isZero: Boolean = {
    map.isEmpty
  }

  //拷贝
  override def copy(): AccumulatorV2[String, mutable.Map[String, Int]] = {
    new AccumulatorDemo001
  }

  //重置
  override def reset(): Unit = {
    map.clear()
  }

  //将每个元素添加到累加器
  override def add(k: String): Unit = {
    val newVal: Int = map.getOrElse(k, 0) + 1
    map.update(k,newVal)
  }

  //在Driver端对从多个Executor收集过来的变量进行merge
  override def merge(other: AccumulatorV2[String, mutable.Map[String, Int]]): Unit = {
    val map1 = this.map
    val map2 = other.value

    // 将map2中的元素合并到map1中
    map2.foreach {
      case(word,count) => {
        val newVal: Int = map1.getOrElse(word,0) + count
        map1.update(word,newVal)
      }
    }
  }

  // 获取累加器的值
  override def value: mutable.Map[String, Int] = this.map
}