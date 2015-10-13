package com.asto.dmp.elem.util

import org.apache.spark.rdd.RDD
import scala.reflect.ClassTag
/**
 * 该类中定义的是跟项目的业务无关的一些共用方法。这些方法放入到DateUtils和FileUtils中是不合适的。这些方法必须具有通用性。自己能够用到的，并且其他同事也可能用到的方法。且在未来的项目中也能使用。
 */
object Utils {

  def otherFunction1() = {
    //.....
  }

  def otherFunction2() = {
    //.....
  }

  def otherFunction3() = {
    //.....
  }

  //对RDD的元素是二元组的RDD提供自定义函数的功能
  class RichPairRDD[K: Ordering : ClassTag, V](rdd: RDD[_ <: Product2[K, V]]) {
    //对元素是二元组的RDD按Key排序后打印出来
    def sortAndPrint(ascending: Boolean = true): Unit = {
      rdd.sortBy(t => t._1, ascending).collect().foreach(println)
    }

    def sortAndPrint(): Unit = sortAndPrint(true)

  }

  implicit def rdd2RichPairRDD[K: Ordering : ClassTag, V](rdd: RDD[_ <: Product2[K, V]]): RichPairRDD[K, V] = new RichPairRDD(rdd)

  class RichRDD[T](rdd: RDD[T]) {
    //打印RDD的每个元素
    def print(): Unit = {
      rdd.collect().foreach(println)
    }
  }
  
  implicit def rdd2RichRDD[T](rdd: RDD[T]): RichRDD[T] = new RichRDD(rdd)
}