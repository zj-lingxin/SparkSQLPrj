package com.asto.dmp.elem.temptable

import com.asto.dmp.elem.base.{BaseContext, Constant, DataSource}
import com.asto.dmp.elem.util.DateUtils
import org.apache.spark.sql.Row

object CreditTable extends DataSource with scala.Serializable  {

  def registerTradeView() = {
    val oneYearAgo = DateUtils.monthsAgo(13,"yyyy-MM-dd hh:mm:ss")
    val tradeViewRowRDD = BaseContext.getSparkContext().textFile(Constant.PATH_TBCM_TRADE_OVER_VIEW).
      map(_.split(",")).filter(_.length == 16).filter(t => "ALL_MONTH".equals(t(4).trim)).
      filter(_(8).matches("^(\\-|\\+)?\\d+(\\.\\d+)?$")).filter(t => t(3).trim > oneYearAgo ).
      map(t => Row(t(0).trim, t(1).trim, t(2).trim, t(3).trim, t(8).trim.toDouble))
    BaseContext.getSqlContext().createDataFrame(tradeViewRowRDD, getSchema(Constant.CREDIT_TRADE_VIEW_SCHEMA)).
      registerTempTable("tbcm_trade_over_view")

    val tradeRowRDD = BaseContext.getSparkContext().textFile(Constant.PATH_TBCM_TRADE).
      map(_.split(",")).filter(_.length == 33).filter(t => "MONTH".equals(t(4).trim)).
      filter(_(11).matches("^(\\-|\\+)?\\d+(\\.\\d+)?$")).filter(t => t(3).trim > oneYearAgo ).
      map(t => Row(t(0).trim, t(1).trim, t(2).trim, t(3).trim, t(11).trim.toDouble))

    BaseContext.getSqlContext().createDataFrame(tradeRowRDD, getSchema(Constant.CREDIT_TRADE_SCHEMA)).registerTempTable("tbcm_trade")
  }

  //注册Beta表
  def registerBeta() = {
    //beta表中如果含有null的字段表示是无穷大，该函数将null字段转化成Int类型的最大值
    def nullToMaxInt(value: String) = if ("null".equals(value)) Integer.MAX_VALUE else value.toInt
    val betaRowRDD = BaseContext.getSparkContext().textFile(Constant.PATH_BETA).map(_.split(",")).filter(_.length == 7).
      map(t => Row(t(1).trim, nullToMaxInt(t(2).trim), t(3).trim.toInt, nullToMaxInt(t(4).trim), t(5).trim.toInt, t(6).trim.toDouble))
    BaseContext.getSqlContext().createDataFrame(betaRowRDD, getSchema(Constant.CREDIT_BETA_SCHEMA)).registerTempTable("beta")
  }

  //注册shop表
  def registerShop() = {
    import com.asto.dmp.elem.util.TempUtils._
    val shopRowRDD = BaseContext.getSparkContext().textFile(Constant.PATH_PROPERTY_SHOP).
      map(_.split(",")).filter(_.length == 19).
      map(a => (a(4).trim, (a(2).trim, a(3).trim))). //(3C数码,(2c01b6044e0f48348251fca53bb714fa,美的天天购专卖店))
      addIndustryType.                               //(3C数码,3C,(2c01b6044e0f48348251fca53bb714fa,美的天天购专卖店))
      map(t => Row(t._3._1, t._3._2, t._1, t._2))    //(2c01b6044e0f48348251fca53bb714fa,美的天天购专卖店,3C数码,3C) / (property_uuid, shop_name, major_business, industry_type)
    BaseContext.getSqlContext().createDataFrame(shopRowRDD, getSchema(Constant.CREDIT_PROPERTY_SHOP_SCHEMA)).registerTempTable("property_shop")
  }

  def registerRefundInfo() = {
    val refundInfoRowRDD = BaseContext.getSparkContext().textFile(Constant.PATH_DATAG_SYCM_DSR).
    map(_.split(",")).filter(_.length == 21).
    filter(a => a(2) >= DateUtils.daysAgo(92,"yyyy-MM-dd hh:mm:ss") && a(2) < DateUtils.getStrDate("yyyy-MM-dd hh:mm:ss")).
    map(a => Row(a(1), a(14).toDouble))
    BaseContext.getSqlContext().createDataFrame(refundInfoRowRDD, getSchema(Constant.CREDIT_REFUND_INFO_SCHEMA)).registerTempTable("refund_info")
  }
}


