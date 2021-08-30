package com.shufang.framework.common

import com.shufang.framework.util.EnvUtil

trait TDao {

  def readFile(path: String) = {
    EnvUtil.take().textFile(path)
  }
}
