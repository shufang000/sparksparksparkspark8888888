package com.shufang.parallel_yuanli

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object RddFromFile {
  def main(args: Array[String]): Unit = {

    val conf: SparkConf = new SparkConf().setMaster("local[8]").setAppName("parellel")
      .set("spark.default.parallelism", "5")

    val sc: SparkContext = new SparkContext(conf)


    /**
     * TODO 1 分区数确定：文件的分区数与minPartition这个参数有关，通常可以通过textFile指定最小分区数，但这个并不是最终分区数量！！
     * TODO 1.1 如果没有指定，那么minPartitions = defaultMinPartitions = math.min(defaultParallelism,2)
     * def textFile(
     * path: String,
     * minPartitions: Int = defaultMinPartitions): RDD[String] = withScope {
     * assertNotStopped()
     * hadoopFile(path, classOf[TextInputFormat], classOf[LongWritable], classOf[Text],
     * minPartitions).map(pair => pair._2.toString).setName(path)
     * }
     * TODO 1.2 很显然 Spark读取数据按照Hadoop TextInputFormat的方式进行读取，所以是按照行读取，分区计算方式如下：
     * extends FileInputFormat<LongWritable, Text>，进入FileInputFormat的 getSplits()获取切片的方法
     *  - totalSize ： 文件的总字节大小  <= totalSize = file.getLength();
     *  - goalSize = totalSize / (long)(numSplits == 0 ? 1 : numSplits); numSplits就是minPartitions
     * TODO 1.3 goalSize就是最终的分区存储的字节数量（如果能整除），假如 totalSize = 7 ，minPartitions（numSplits） = 2
     * => goalSize = 7 / 2 =  3 ... 1 ，现在每个分区的字节数为3，余数为1，按照Hadoop的分区规则，1+3/3 > 1.1
     * => 最终RDD的分区个数为：2 + 1 = 3
     *
     * TODO 1.4 files/wordcount.txt为75个字节，包括换行符等，此时使用默认的defaultMinPartitions = 2
     * => totalSize = 75
     * => goalSize = 75/2 = 37...1  1/37 < 0.1 所以最终的分区个数为2
     * =======================================================================================================
     * TODO 2 数据的分区分配
     * TODO 2.1 数据按照行读取，偏移量不会重复读取
     * TODO 2.2 数据分配按照偏移量分配
     * 分区  偏移量    最终读取的行号，第一行22字节，第二行22字节，第三行31字节
     * 分区0 [0,37]   N1、N2
     * 分区1 [38,75]  N3
     * TODO 所以最终的数据第1，2行被分配到第一个分区文件
     * part-00000
     * spark   spark   hello
     * flink   world   hello
     * TODO 第三行被分配到第二个分区文件
     * part-00001
     * good man    good    good good
     *
     */
    val rdd: RDD[String] = sc.textFile("files/wordcount.txt",3)

    rdd.saveAsTextFile("output")

    sc.stop()
  }
}
