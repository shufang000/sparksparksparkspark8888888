package com.shufang.framework.service

import com.shufang.framework.common.TService
import com.shufang.framework.dao.WordCountDao
import org.apache.spark.rdd.RDD

class WordCountService extends TService {

  private val dao = new WordCountDao()

  // TODO 进行逻辑操作,将dataAnalysis进行抽象
  override def dataAnalysis() = {
    val sourceRdd: RDD[String] = dao.readFile("files/ads.txt")
    val rdd: RDD[String] = sourceRdd.flatMap(_.split("\\s+"))
    val value: RDD[(String, Int)] = rdd.map((_, 1))
    val wordCountRDD: RDD[(String, Int)] = value.reduceByKey(_ + _)

    wordCountRDD
  }
}
