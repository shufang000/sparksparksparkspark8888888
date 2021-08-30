package com.shufang.framework.common

import com.shufang.framework.util.EnvUtil
import org.apache.spark.{SparkConf, SparkContext}

trait TApplication {

  // TODO 控制抽象
  def start(master: String = "local[*]", appName: String = "WordCount")(op: => Unit): Unit = {
    val conf: SparkConf = new SparkConf().setMaster(master).setAppName(appName)
    val sc = new SparkContext(conf)
    EnvUtil.put(sc)

    try {
      op
    } catch {
      case ex => println(ex.getMessage)
    }

    sc.stop()
    EnvUtil.clear()
  }
}
