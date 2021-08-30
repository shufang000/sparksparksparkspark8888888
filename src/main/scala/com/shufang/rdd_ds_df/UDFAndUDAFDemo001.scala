package com.shufang.rdd_ds_df

import com.shufang.utils.ScUtil
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, Dataset, SparkSession, functions}

object Transfer {
  def main(args: Array[String]): Unit = {

    val sc: SparkContext = ScUtil.getSc

    val spark: SparkSession = SparkSession.builder().config(sc.getConf).getOrCreate()

    import spark.implicits._

    val rdd: RDD[(Int, String)] = sc.makeRDD(List((1001, "shufang"), (1001, "shufang"), (1002, "lisi")))
    rdd.cache()

    val df: DataFrame = rdd.toDF("id", "name")


    // 注册UDF、UDAF
    spark.udf.register("prefix1", (name: String) => {
      "Name:" + name
    })
    spark.udf.register("ageAvg", functions.udaf(new MyUDAF1()))
    //spark.udf.register("ageAvg", new MyUDAF)


    df.createOrReplaceTempView("users")

    spark.sql("select *,prefix1(name) from users").show()
    spark.sql("select ageAvg(id) as av from users").show()
    spark.sql("select ageAvg(id) as av from users").show()
    spark.stop()
    sc.stop()
  }
}

case class User(var id: Int, var name: String)
