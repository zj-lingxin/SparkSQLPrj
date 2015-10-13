package com.asto.dmp.elem.base

import com.asto.dmp.elem.util.logging.Logging
import org.apache.spark.sql.SQLContext
import org.apache.spark.sql.hive.HiveContext
import org.apache.spark.{SparkConf, SparkContext}

object BaseContext extends Logging with scala.Serializable {
  private var _sc: SparkContext = _
  private var _hiveContext: HiveContext = _
  private var _sqlContext: SQLContext = _

  def getHiveContext: HiveContext = {
    if (_hiveContext == null) {
      logInfo("对HiveContext进行实例化")
      _hiveContext = new HiveContext(getSparkContext)
    }
    _hiveContext
  }

  def getSqlContext: SQLContext = {
    if (_sqlContext == null) {
      logInfo("对SQLContext进行实例化")
      _sqlContext = new SQLContext(getSparkContext)
    }
    _sqlContext
  }

  def getSparkContext: SparkContext = {
    if (_sc == null) {
      logInfo("对SparkContext进行实例化")
      _sc = initSparkContext()
    }
    _sc
  }

  def initSparkContext(master: String = null): SparkContext = {
    val conf = new SparkConf().setAppName(Constants.App.SPARK_UI_APP_NAME)
    val masterInCodes = Option(master)
    val masterInSparkConf = conf.getOption("spark.master")
    (masterInCodes, masterInSparkConf) match {
      case (None, None) =>
        logWarn(s"集群和程序代码中都没有设置Master参数,在${getClass.getName}的initSparkContext中对它设置成local[2]")
        conf.setMaster("local[2]")
      case (None, Some(_)) =>
        logInfo(s"程序代码中都没有设置Master参数,但是集群中设置了Master参数，使用集群设置的Master参数")
      case (Some(_), None) =>
        logInfo(s"集群中没有设置Master参数，但是程序代码中都设置了Master参数,使用程序代码的Master参数")
        conf.setMaster(masterInCodes.get)
      case (Some(_), Some(_)) =>
        logInfo(s"集群中设置了Master参数，程序代码中也设置了Master参数,程序代码的Master参数覆盖集群传入的Master参数")
        conf.setMaster(masterInCodes.get)
    }
    logInfo(s"Master = ${conf.get("spark.master")},conf = ${conf.get("spark.app.name")}")

    this._sc = new SparkContext(conf)
    _sc
  }

  def stopSparkContext() = {
    logInfo("关闭SparkContext")
    _sc.stop()
  }
}
