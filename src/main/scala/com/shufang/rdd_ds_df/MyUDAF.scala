package com.shufang.rdd_ds_df

import org.apache.spark.sql.Row
import org.apache.spark.sql.expressions.{MutableAggregationBuffer, UserDefinedAggregateFunction}
import org.apache.spark.sql.types.{DataType, LongType, StructField, StructType}

class MyUDAF extends UserDefinedAggregateFunction {
  // IN
  override def inputSchema: StructType = {
    StructType(
      Array(
        StructField("age", LongType)
      )
    )
  }

  // MIDDLE
  override def bufferSchema: StructType = {
    StructType(
      Array(
        StructField("total", LongType),
        StructField("count", LongType)
      )
    )
  }

  // OUT
  override def dataType: DataType = LongType

  // 函数的稳定性
  override def deterministic: Boolean = {
    true
  }

  // 缓冲器的初始化
  override def initialize(buffer: MutableAggregationBuffer): Unit = {
    /*buffer(0) = 0L
    buffer(1) = 0L*/
    buffer.update(0, 0L)
    buffer.update(1, 0L)
  }

  // 根据输入的值更新缓冲区
  override def update(buffer: MutableAggregationBuffer, input: Row): Unit = {
    buffer.update(0, buffer.getLong(0) + input.getLong(0))
    buffer.update(1, buffer.getLong(1) + 1)
  }

  // 合并多个缓冲区
  override def merge(buffer1: MutableAggregationBuffer, buffer2: Row): Unit = {
    buffer1.update(0,buffer1.getLong(0) + buffer2.getLong(0))
    buffer1.update(1,buffer1.getLong(1) + buffer2.getLong(1))
  }

  // 计算平均值
  override def evaluate(buffer: Row): Any = {
    buffer.getLong(0)/buffer.getLong(1)
  }
}
