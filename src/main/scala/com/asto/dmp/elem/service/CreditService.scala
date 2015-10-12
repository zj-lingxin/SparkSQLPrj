package com.asto.dmp.elem.service

import com.asto.dmp.elem.base._
import com.asto.dmp.elem.dao.CreditDao
import com.asto.dmp.elem.temptable.CreditTable
import com.asto.dmp.elem.util.logging.Logging
import com.asto.dmp.elem.util.mail.{MailAgent, Mail}
import com.asto.dmp.elem.util.FileUtils

class CreditService extends DataSource with scala.Serializable with Logging {

  import sqlContext.implicits._

  def run(): Unit = {
    try {

      FileUtils.deleteHdfsFile(Constant.PATH_CREDIT_RESULT_TEXT)
      FileUtils.deleteHdfsFile(Constant.PATH_CREDIT_RESULT_PARQUET)

      val sc = BaseContext.getSparkContext()
      val creditDao = new CreditDao()

      CreditTable.registerTradeView
      val totalSales = creditDao.totalSales

      CreditTable.registerBeta
      val betaInfo = creditDao.betaInfo

      CreditTable.registerShop
      val shopInfo = creditDao.shopInfo

      CreditTable.registerRefundInfo
      val refundRate = creditDao.refundRate

      //val shuadanRate = creditDao.shuadanRate
      val shuadanRate = sc.parallelize(Array(
        ("2c01b6044e0f48348251fca53bb714fa", "0.12"),
        ("326c29ae272c11e5881b16ae6eea2308", "0.03"),
        ("3accb02f88f7499381c70885affc7a66", "0.23"),
        ("5b2789a7748f4319889683e4a8929da0", "0.133"),
        ("f0255ffe3da5434194badb1783ffe558", "0.0"),
        ("fcbd34e9e66640f0be797435e6bd00ab", "0.034")
      )).map(a => (a._1.toString, a._2.toDouble))

      // 大型活动暂时不考虑。
      // 这边暂时假设大型活动获取到的数据类似 Array[(Long, Double)]，定义bigEventSalesArray = 大型活动产生的交易额。
      // 数组中二元组的第一个元素表示shop_id,第二个元素表示大型活动的销售额
      val bigEventSales = sc.parallelize(Array[(String, Double)]())
      // 定义 minusBigEventSales = (前12个月销售总额-大型活动产生的交易额)
      // totalSales外连接bigEventSales。得到类似(shop_id,(销售总额，大型活动销售额))这样的数据，但是如果该shop_id没有“大型活动销售额”那么得到的数据是这样的(shop_id,(销售总额，None))
      // =>  将(shop_id,(销售总额，None))改成(shop_id,(销售总额，0))
      // =>  得到(shop_id,(销售总额-大型活动销售额,大型活动销售额))这样的数据
      val minusBigEventSales = totalSales.leftOuterJoin(bigEventSales).map(t => Pair[String, (Double, Double)](t._1, (t._2._1, t._2._2.getOrElse(0)))).map(t => (t._1, (t._2._1 - t._2._2, t._2._2)))

      // 定义 minusClickFarmingSales = [(前12个月销售总额-大型活动产生的交易额)*(1-刷单比率)]
      // (shop_id,(销售总额-大型活动销售额,大型活动销售额))
      // =>  (shop_id,((销售总额-大型活动销售额,大型活动销售额),  刷单比率)
      // 例如 (   7000,((    1.850615838000001E7,       10000.0),Some(0.12))
      // =>  (shop_id,((销售总额-大型活动销售额,大型活动销售额),(1-刷单比率,刷单比率))   注意：刷单比率≤10%，不做虚假交易比率和虚假金额扣减
      // 例如 (    7000,((    1.850615838000001E7,       10000.0),(      0.88,    0.12))     即：刷单比率≤10%时，1-刷单比率 = 1.0
      // =>  (shop_id,([(销售总额-大型活动销售额)*(1-刷单比率)],大型活动销售额,刷单比率))
      // 例如 (   7000,(                    1.6285419374400008E7,       10000.0,    0.12))
      val minusClickFarmingSales = minusBigEventSales.leftOuterJoin(shuadanRate).map { t =>
        if (t._2._2 == None || t._2._2.get <= 0.1)
          Pair[String, ((Double, Double), (Double, Double))](t._1, (t._2._1, (1, t._2._2.getOrElse(0))))
        else
          Pair[String, ((Double, Double), (Double, Double))](t._1, (t._2._1, ((1 - t._2._2.get), t._2._2.get)))
      }.map(t => (t._1, (t._2._1._1 * t._2._2._1, t._2._1._2, t._2._2._2)))

      // (shop_id,([(销售总额-大型活动销售额)*(1-刷单比率)],大型活动销售额,刷单比率))   //minusClickFarmingSales
      // 例如(   7000,(                    1.6285419374400008E7,       10000.0,    0.12))
      // => (shop_id,(([(销售总额-大型活动销售额)*(1-刷单比率)],大型活动销售额,刷单比率),            Some(退款率))
      // 例如(   7000,((                    1.6285419374400008E7,       10000.0,    0.12),Some(0.4632258064516129))
      // => (shop_id,(         resultSales,大型活动销售额,刷单比率,            退款率)  //定义resultSales = [(前12个月销售总额-大型活动产生的交易额)*(1-刷单比率)*（1-退款率）]
      // 例如(   7000,(1.6209981109168917E7,       10000.0,    0.12,0.4632258064516129)  //注意退款率的单位是 百分比
      val salesInfo = minusClickFarmingSales.leftOuterJoin(refundRate).
        map { t => (t._1, (t._2._1._1 * (1 - t._2._2.getOrElse(0).toString.toDouble * 0.01), t._2._1._2, t._2._1._3, t._2._2.getOrElse(0).toString.toDouble)) }.
        map(t => if (t._2._1 < 0) Pair[String, (Double, Double, Double, Double)](t._1, (0D, t._2._2, t._2._3, t._2._4)) else t)

      // score = 评分卡中的得分，暂时这个得分还不能获取，下面为假数据 元组的第一个元素是shop_id,第二个元素是得分score
      // val score = sc.parallelize(Array[(Long, Int)]((7000, 551),(9631, 551), (7231, 513), (7431, 576), (6431, 499), (8431, 601), (5031, 532), (9431, 544)))

      //val score = sc.textFile(CommonDefinition.PATH_SCORE_RESULT_TEXT).map(_.split(",")).map(t => (t(0), t(1).toDouble + 500))
      val score = sc.parallelize(Array(
        ("2c01b6044e0f48348251fca53bb714fa", "546"),
        ("326c29ae272c11e5881b16ae6eea2308", "513"),
        ("3accb02f88f7499381c70885affc7a66", "655"),
        ("5b2789a7748f4319889683e4a8929da0", "533"),
        ("f0255ffe3da5434194badb1783ffe558", "576"),
        ("fcbd34e9e66640f0be797435e6bd00ab", "554")
      )).map(a => (a._1, a._2.toDouble))

      // (shop_id,(         resultSales,大型活动销售额,刷单比率,            退款率)  //定义resultSales = [(前12个月销售总额-大型活动产生的交易额)*(1-刷单比率)*（1-退款率）]
      // 例如(   7000,(1.6209981109168917E7,       10000.0,    0.12,0.4632258064516129)  //注意退款率的单位是 百分比
      // =>  (shop_id,((         resultSales,大型活动销售额,刷单比率,            退款率),Some(industry_type))
      // 例如(   7000,((1.6209981109168917E7,       10000.0,    0.12,0.4632258064516129),         Some(其他))
      // =>  (shop_id,((          resultSales,大型活动销售额,刷单比率,            退款率),Some(industry_type)),Some(score))
      // 例如(7000   ,(((1.6209981109168917E7,       10000.0,    0.12,0.4632258064516129),        Some(其他)), Some(551))) //注意，如果找不到得分，那么score是None
      // =>  (industry_type,(shop_id，(          resultSales,大型活动销售额,刷单比率,            退款率),score)
      // 例如(其他,         (7000,    ( 1.6209981109168917E7,       10000.0,    0.12,0.4632258064516129),551.0))
      // =>  (industry_type,((shop_id,(         resultSales,大型活动销售额,刷单比率,            退款率),score),Some((sales_upper_limit,sales_lower_limit,score_upper_limit,score_lower_limit,beta_score))))
      // 例如(其他         ,((7000   ,(1.6209981109168917E7,       10000.0,    0.12,0.4632258064516129),551.0),Some((              300,                0,              510,                 0,      0.06))))，但是这边相同的shop_id的数据是多个的，只有一个是符合要求的
      // =>  (industry_type,((shop_id,(         resultSales,大型活动销售额,刷单比率,            退款率),score),Some((sales_upper_limit,sales_lower_limit,score_upper_limit,score_lower_limit,beta_score))))   //过滤出符合条件的那条数据
      // 例如(其他         ,((7000   ,(1.6209981109168917E7,       10000.0,    0.12,0.4632258064516129),551.0),Some((             2000,              800,              559,                551,     0.16))))
      // =>  (shop_id,         resultSales,  β, industry_type, score,大型活动销售额,刷单比率,            退款率)
      // 例如(   7000,1.6209981109168917E7,0.16,          其他, 551.0,       10000.0,    0.12,0.4632258064516129)
      // =>	 (shop_id,         resultSales,  β, industry_type,score,sales*β/12*授信月份数,行业风险限额,大型活动销售额,刷单比率,            退款率)
      // 例如(   7000,1.6209981109168917E7,0.16,          其他,551.0,    1296798.4887335133,    250000.0,       10000.0,    0.12,0.4632258064516129)
      //  =>  (shop_id, (         resultSales,  β, industry_type,score,sales*β/12*授信月份数,行业风险限额,产品限额,MIN{resultSales*β/12*授信月份数,行业风险限额,产品限额}，大型活动销售额,刷单比率,             退款率))
      // 例如(   7000, (1.6209981109168917E7,0.16,          其他,551.0,    1296798.4887335133,    250000.0,  500000,                                               250000.0,        10000.0,    0.12, 0.4632258064516129))
      val tmpResult = salesInfo.leftOuterJoin(shopInfo).leftOuterJoin(score).
        map(t => (t._2._1._2.getOrElse("其他"), (t._1, t._2._1._1, t._2._2.getOrElse(500).toString.toDouble))).
        leftOuterJoin(betaInfo).filter( t => t._2._1._2._1 / 10000 >= t._2._2.get._2 && t._2._1._2._1 / 10000 < t._2._2.get._1 && t._2._1._3 >= t._2._2.get._4 && t._2._1._3 < t._2._2.get._3).
        map(t => (t._2._1._1, t._2._1._2._1, t._2._2.get._5, t._1, t._2._1._3, t._2._1._2._2, t._2._1._2._3, t._2._1._2._4)).
        map(t => (t._1, t._2, t._3, t._4, t._5, t._2 * t._3 / 12 * CreditService.shouXinMonths, CreditService.getRiskLimits(t._4, t._2).toDouble, t._6, t._7, t._8)).
        map(t => (t._1, (t._2, t._3, t._4, t._5, t._6, t._7, CreditService.productLimits, Array(t._6, t._7, CreditService.productLimits).min, t._8, t._9, t._10)))

      // Transformations过程：totalSales = 前12个月销售总额
      // (shop_id,totalSales)  // 例如 (7000,1.851615838000001E7)
      // =>  (shop_id,((         totalSales, totalSales/12*1.5),Some((         resultSales,  β, industry_type,score,sales*β/12*授信月份数, 行业风险限额,产品限额,MIN{resultSales*β/12*授信月份数,行业风险限额,产品限额}，大型活动销售额, 刷单比率,            退款率)))
      // 例如 (   7000,((1.851615838000001E7,2314519.7975000013),Some((1.6209981109168917E7,0.16,          其他,551.0,    1296798.4887335133,     250000.0,  500000,                                               250000.0,        10000.0,     0.12,0.4632258064516129))))
      // =>  (shop_id,          totalSales,         resultSales,  β, industry_type, score, resultSales*β/12*授信月份数,  totalSales/12*1.5, 行业风险限额, 产品限额,大型活动销售额, 刷单比率,            退款率, MIN{resultSales*β/12*授信月份数,totalSales/12*1.5,行业风险限额,产品限额})
      //     (   7000, 1.851615838000001E7, 1.851615838000001E7, 0.1,          其他,   0.0,      925807.9190000006, 2314519.7975000013,     250000.0,   500000,       10000.0,     0.12,0.4632258064516129,                                                            250000.0)
      // 最后输出的字段分别是：
      // #1# shop_id, #2# 前12个月销售总额，#3# [(前12个月销售总额-大型活动产生的交易额)*(1-刷单比率)*（1-退款率）]，#4# β，
      // #5# 行业类型，#6# 得分，#7# [(前12个月销售总额-大型活动产生的交易额)*(1-刷单比率)*（1-退款率）]*β/12*授信月份数，#8# 前12个月销售总额/12*1.5，
      // #9# 行业风险限额, #10# 产品限额, #11# 大型活动销售额,( /*t._2._2.get._9,*/ 去掉！) #12# 刷单比率,
      // #13# 退款率, #14# MIN{[(前12个月销售总额-大型活动产生的交易额)*(1-刷单比率)*（1-退款率）]*β/12*授信月份数,totalSales/12*1.5,行业风险限额,产品限额}
      val result = totalSales.map(t => (t._1, (t._2, t._2 / 12 * 1.5))).leftOuterJoin(tmpResult).map( t => (t._1, t._2._1._1, t._2._2.get._1, t._2._2.get._2, t._2._2.get._3, t._2._2.get._4,
        t._2._2.get._5, t._2._1._2, t._2._2.get._6, t._2._2.get._7, t._2._2.get._10, t._2._2.get._11, Array(t._2._1._2, t._2._2.get._8).min))
      result.toDF("shop_id", "前12个月销售总额", "resultSales", "β", "行业类型", "得分", "resultSales*β/12*授信月份数",
        "前12个月销售总额/12*1.5", "行业风险限额", "产品限额", "刷单比率", "退款率", "授信额度").write.parquet(Constant.PATH_CREDIT_RESULT_PARQUET)
      result.map(_.productIterator.mkString(",")).coalesce(1, true).saveAsTextFile(Constant.PATH_CREDIT_RESULT_TEXT)

    } catch {
      case t: Throwable =>
        MailAgent(t, Constant.MAIL_CREDIT_SUBJECT, Mail.getPropByKey("mail_to_credit")).sendMessage()
        error(Constant.MAIL_CREDIT_SUBJECT, t)
    }
  }
}

object CreditService {
  //行业风险限额的确定。修改之后：行业风险限额=行业风险限额（原）*2
  def getRiskLimits(industry_type: String, sales: Double) = {
    val s = sales / 10000 //转化为以万为单位
    if ("3C".equals(industry_type)) {
      if (s >= 15000) 2000000
      else if (s >= 8000) 1000000
      else if (s >= 3000) 800000
      else if (s >= 1000) 600000
      else if (s >= 500) 400000
      else 200000
    } else {
      if (s >= 10000) 2000000
      else if (s >= 5000) 1000000
      else if (s >= 2000) 700000
      else if (s >= 800) 500000
      else if (s >= 300) 400000
      else 300000
    }
  }

  //定义 shouXinMonths = 授信月份数
  val shouXinMonths = 6
  //定义 productLimits = 产品限额 ，定为50w
  val productLimits = 500000
}
