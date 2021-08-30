package com.shufang.framework.application

import com.shufang.framework.common.TApplication
import com.shufang.framework.controller.WordCountController


object WordCountApplication extends App with TApplication {
  start("local[*]","WordCount") {
    val controller = new WordCountController
    controller.dispatch()
  }
}
