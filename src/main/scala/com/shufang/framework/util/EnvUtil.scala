package com.shufang.framework.util

import org.apache.spark.SparkContext

object EnvUtil {

  // TODO 开辟一个单线程的共享内存，因为从Application开始都是在一个线程中执行的不会存在线程安全的问题!
  private val threadLocal = new ThreadLocal[SparkContext]()

  // 装入一个数据
  def put(sc:SparkContext):Unit = {
    threadLocal.set(sc)
  }

  def take():SparkContext = {
    threadLocal.get()
  }

  def clear():Unit = {
    threadLocal.remove()
  }
}
