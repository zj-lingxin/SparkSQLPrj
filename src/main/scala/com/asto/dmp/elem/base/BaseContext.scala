package com.asto.dmp.elem.base

import org.apache.spark._
import org.apache.spark.sql.SQLContext
import org.apache.spark.sql.hive.HiveContext

object BaseContext extends scala.Serializable {
  private var _sc: SparkContext = _
  private var _hiveContext: HiveContext = _
  private var _sqlContext: SQLContext = _

  def initSparkContext(master: String = null): SparkContext = {
    val conf = new SparkConf().setAppName(Constant.APP_NAME)
    if (master != null) conf.setMaster(master)
    this._sc = new SparkContext(conf)
    _sc
  }

  def getSparkContext(): SparkContext = {
    if (_sc == null)
      _sc = initSparkContext()
    _sc
  }

  def getHiveContext(): HiveContext = {
    if (_hiveContext == null)
      _hiveContext = new HiveContext(getSparkContext())
    _hiveContext
  }

  def getSqlContext(): SQLContext = {
    if (_sqlContext == null)
      _sqlContext = new SQLContext(getSparkContext())
    _sqlContext
  }

  def stopSparkContext() = _sc.stop
}
