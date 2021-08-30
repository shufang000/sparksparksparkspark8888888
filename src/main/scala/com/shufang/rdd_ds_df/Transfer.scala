package com.shufang.rdd_ds_df

import com.shufang.utils.ScUtil
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, Dataset, SparkSession, TypedColumn, functions}

object Transfer {
  def main(args: Array[String]): Unit = {

    val sc: SparkContext = ScUtil.getSc

    val spark: SparkSession = SparkSession.builder().config(sc.getConf).getOrCreate()

    import spark.implicits._

    val rdd: RDD[(Int, String)] = sc.makeRDD(List((1001, "shufang"), (1001, "shufang"), (1002, "lisi")))
    rdd.cache()

    val df: DataFrame = rdd.toDF("id", "name")
    /*    df.show()

        val df6: DataFrame = df.select($"id" + 1, $"name")
        val df7: DataFrame = df.select('id + 1, 'name)
        val df8: DataFrame = df.select("name", "id")





        df6.show()
        df7.show()
        df8.show()
        df.dropDuplicates().show()*/


    /*
        val ds: Dataset[User] = df.as[User]
        ds.show()

        val rdd1: RDD[User] = ds.rdd
        println(rdd1.collect().mkString(","))

        val ds2: Dataset[(Int, String)] = rdd.toDS()
        ds2.show()

        val df1: DataFrame = ds.toDF()
        df1.show()

        val df2: DataFrame = List((1001, "shufang"), (1002, "lisi")).toDF("id", "name")
        df2.show()
    */

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


    val column: TypedColumn[User, Long] = new MyUDAF2().toColumn
    val ds: Dataset[User] = df.as[User]
    ds.select(column).show()

    spark.stop()
    sc.stop()
  }
}

case class User(var id: Int, var name: String)
