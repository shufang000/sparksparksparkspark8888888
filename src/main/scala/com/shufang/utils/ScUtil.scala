package com.shufang.utils

import org.apache.spark.{SparkConf, SparkContext}

object ScUtil {
  def getSc: SparkContext = {
    val sc_conf: SparkConf = new SparkConf().setMaster("local[6]").setAppName("sc_conf")
    new SparkContext(sc_conf)
  }
}
