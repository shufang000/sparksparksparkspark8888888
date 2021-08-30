package com.shufang.framework.controller

import com.shufang.framework.common.TController
import com.shufang.framework.service.WordCountService
import org.apache.spark.rdd.RDD


class WordCountController extends TController {

  private val service = new WordCountService()

  // TODO 执行业务请求操作，进行调度,这可以抽象到Trait中
  override def dispatch(): Unit = {
    val wordCountRDD: RDD[(String, Int)] = service.dataAnalysis()
    wordCountRDD.collect().foreach(println(_))
  }

}
