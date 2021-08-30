package com.shufang.rdd_ds_df

import org.apache.spark.sql.{Encoder, Encoders, Row}
import org.apache.spark.sql.expressions.{Aggregator, MutableAggregationBuffer, UserDefinedAggregateFunction}

/**
 * Aggregator[IN, BUF, OUT] should now be registered as a UDF" + via the functions.udaf(agg) method.", "3.0.0"
 */
//case class Buff(var total:Long ,var count:Long)
class MyUDAF2 extends Aggregator[User,Buff,Long] {
  //缓冲区初始化
  override def zero: Buff = Buff(0L,0L)

  override def reduce(b: Buff, a: User): Buff = {
    b.count +=1
    b.total += a.id
    b
  }

  override def merge(b1: Buff, b2: Buff): Buff = {
    b1.count  = b1.count + b2.count
    b1.total  = b1.total + b2.total
    b1
  }

  override def finish(buff: Buff): Long = {
    buff.total/buff.count
  }

  override def bufferEncoder: Encoder[Buff] = Encoders.product

  override def outputEncoder: Encoder[Long] = Encoders.scalaLong
}
