package com.shufang.parallel_yuanli

import com.shufang.utils.ScUtil
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.rdd.RDD

/**
 */
object RddFromMemoryCollection {
  def main(args: Array[String]): Unit = {

    val conf: SparkConf = new SparkConf().setMaster("local[8]").setAppName("parellel")
      .set("spark.default.parallelism", "5")

    val sc: SparkContext = new SparkContext(conf)


    /**
     * TODO makeRDD() 底层调用的就是 parallelize()
     * TODO 1 ：如何确定分区的数量
     * 通常numSlices代表RDD的分区的个数，那么这个分区的个数呢可以手动指定，也可以使用默认值
     * 当手动指定时，RDD的分区个数：numSlices
     * 如果使用默认值，RDD的分区个数：numSlices => numSlices = defaultParallelism = defaultParallelism()
     * def defaultParallelism: Int = {
     * assertNotStopped()
     * taskScheduler.defaultParallelism
     * }
     *
     * taskScheduler.defaultParallelism =
     * override def defaultParallelism(): Int =
     * backend.defaultParallelism()
     *
     * override def defaultParallelism(): Int =
     * scheduler.conf.getInt("spark.default.parallelism", totalCores)
     * totalCores是当前环境master能够使用的最大核数，比如totalCores = local[*]或者
     * set spark.executor.cores = 5;
     * set executor.nums = 3;
     * =>>>>>> totalCores = 15
     * ==================================================================================
     * TODO 2 ：如何给数据找准对应的分区进行分配
     * def parallelize[T: ClassTag](
     * seq: Seq[T],
     * numSlices: Int = defaultParallelism): RDD[T] = withScope {
     * assertNotStopped()
     * TODO 2.1 这是主要的数据分区分配的入口
     * new ParallelCollectionRDD[T](this, seq, numSlices, Map[Int, Seq[String]]())
     * }
     * TODO 2.2 找到每个分区的元素的下标范围：[start,end)
     * def positions(length: Long, numSlices: Int): Iterator[(Int, Int)] = {
     * (0 until numSlices).iterator.map { i =>
     * val start = ((i * length) / numSlices).toInt
     * val end = (((i + 1) * length) / numSlices).toInt
     * (start, end)
     * }
     * }
     * TODO 2.3 seq是传入的集合,然后将范围内的元素进行切分给不同的分区
     * case _ =>
     * val array = seq.toArray // To prevent O(n^2) operations for List etc
     * positions(array.length, numSlices).map { case (start, end) =>
     * array.slice(start, end).toSeq
     * }.toSeq
     * TODO 当前有 5 个元素，分区数为2
     * 分区0  [0*5/2,1*5/2) => [0,2)  => (1,2)
     * 分区1  [1*5/2,2*5/2) => [2,5)  => (3,4,5)
     * 所以最终输出产生2个文件：
     * part-00000
     * 1
     * 2
     * part-00001
     * 3
     * 4
     * 5
     *
     */
    val rdd: RDD[Int] = sc.makeRDD(List(1, 2, 3, 4, 5), 2)

    rdd.saveAsTextFile("output")

    sc.stop()
  }
}
