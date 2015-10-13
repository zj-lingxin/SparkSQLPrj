package com.asto.dmp.elem.base

import com.asto.dmp.elem.util.DateUtils

object Constants {

  /** App中的常量与每个项目相关 **/
  object App {
    //spark UI界面显示的名称
    val SPARK_UI_APP_NAME  = "yushanfang_statistics"
    //项目的中文名称
    val CHINESE_NAME       = "生意参谋-御膳房"
    //项目数据存放的Hadoop的目录
    val HADOOP_DIR         = "hdfs://appcluster/sycm/"
    //默认日志文件的路径
    val DEFAULT_LOG_PROPS  = "com/asto/dmp/elem/log4j.properties"
  }

  /** 输入文件路径 **/
  object InputPath {
    private val DIR           = s"${App.HADOOP_DIR}/input"
    val PROPERTY_SHOP          = s"$DIR/property_shop/*"
    val TBCM_TRADE_OVER_VIEW   = s"$DIR/tbcm_trade_over_view/*"
    val TBCM_TRADE             = s"$DIR/tbcm_trade/*"
    val DATAG_SYCM_DSR         = s"$DIR/datag_sycm_dsr/*"
  }

  /** 中间文件路径 **/
  object MiddlePath {
    private val DIR       = s"${App.HADOOP_DIR}/middle"
    val BETA               = s"$DIR/beta/*"
    val INDUSTRY_RELATION  = s"$DIR/industry_relation" //这里不需要“/*”
  }

  /** 输出文件路径 **/
  object OutputPath {
    private val TODAY   = DateUtils.getStrDate("yyyyMM/dd")
    private val DIR              = s"${App.HADOOP_DIR}/output"
    //刷单结果路径
    val FRAUD_PARQUET    = s"$DIR/parquet/$TODAY/fraud"
    //得分结果路径
    val SCORE_TEXT       = s"$DIR/text/$TODAY/score"
    //授信结果路径
    val CREDIT_TEXT      = s"$DIR/text/$TODAY/credit"
    val CREDIT_PARQUET   = s"$DIR/parquet/$TODAY/credit"
  }

  /** 表的模式 **/
  object Schema {
    val CREDIT_TRADE_VIEW    = "id:String,user_id:String,shop_id:String,gmt_target:String,pay_money:Double"
    val CREDIT_TRADE         = "id:String,user_id:String,shop_id:String,gmt_target:String,pay_money:Double"
    val CREDIT_BETA          = "industry_type:String,sales_upper_limit:Int,sales_lower_limit:Int,score_upper_limit:Int,score_lower_limit:Int,beta_score:Double"
    val CREDIT_PROPERTY_SHOP = "property_uuid:String,shop_name:String,major_business:String,industry_type:String"
    val CREDIT_REFUND_INFO   = "shop_id:String,refund_rate:Double"
  }

  /** 邮件发送功能相关常量 **/
  object Mail {
    //以下参数与prop.properties中的参数一致。
    val TO            = "zhengc@asto-inc.com"
    val FROM          = "dmp_notice@asto-inc.com"
    val PASSWORD      = "astodmp2015"
    val SMTPHOST      = "smtp.qq.com"
    val SUBJECT       = s"“${App.CHINESE_NAME}”项目出异常了！"
    val CC            = ""
    val BCC           = ""
    val TO_FRAUD      = "fengtt@asto-inc.com"
    val TO_SCORE      = "yewb@asto-inc.com"
    val TO_APPROVE    = "lij@asto-inc.com"
    val TO_CREDIT     = "lingx@asto-inc.com"
    val TO_LOAN_AFTER = "liuw@asto-inc.com"

    //以下参数prop.properties中没有， MAIL_CREDIT_SUBJECT是授信规则模型出问题时邮件的主题
    val CREDIT_SUBJECT  = s"${App.CHINESE_NAME}-授信规则 项目出现异常，请尽快查明原因！"
    val APPROVE_SUBJECT = s"${App.CHINESE_NAME}-准入规则结果集写入失败，请尽快查明原因！"
  }

}
