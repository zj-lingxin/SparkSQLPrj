package com.asto.dmp.elem.base

import com.asto.dmp.elem.util.DateUtils

object Constant {
  private val now = DateUtils.getStrDate("yyyyMM/dd")
  //spark UI界面显示的名称
  val APP_NAME = "yushanfang_statistics"
  //项目的名称
  val PROJECT_NAME = "生意参谋-御膳房"
  //默认日志文件的路径
  val DEFAULT_LOG_PROPS = "com/asto/dmp/elem/log4j.properties"

  /** +++++++++++++++++hdfs path++++++++++++++++++++++++ */
  val HADOOP_INPUT = "hdfs://appcluster/sycm/input"
  val HADOOP_OUTPUT = "hdfs://appcluster/sycm/output"
  val HADOOP_MIDDLE = "hdfs://appcluster/sycm/middle"

  /** +++++++++++++++++++输入文件路径+++++++++++++++++++++ */
  val PATH_PROPERTY_SHOP = Constant.HADOOP_INPUT + "/property_shop/*"
  val PATH_TBCM_TRADE_OVER_VIEW = Constant.HADOOP_INPUT + "/tbcm_trade_over_view/*"
  val PATH_TBCM_TRADE = Constant.HADOOP_INPUT + "/tbcm_trade/*"
  val PATH_DATAG_SYCM_DSR = Constant.HADOOP_INPUT + "/datag_sycm_dsr/*"

  /** +++++++++++++++++++中间文件的路径+++++++++++++++++++++ */
  val PATH_BETA = Constant.HADOOP_MIDDLE + "/beta/*"
  val PATH_INDUSTRY_RELATION = Constant.HADOOP_MIDDLE + "/industry_relation" //这里不需要“/*”

  /** +++++++++++++++++++输出文件的路径+++++++++++++++++++++ */
  //刷单结果路径
  val PATH_FRAUD_RESULT_PARQUET = Constant.HADOOP_OUTPUT + "/parquet/" + now + "/fraud"
  //得分结果路径
  val PATH_SCORE_RESULT_TEXT = Constant.HADOOP_OUTPUT + "/text/" + now + "/score"
  //授信结果路径
  val PATH_CREDIT_RESULT_TEXT = Constant.HADOOP_OUTPUT + "/text/" + now + "/credit"
  val PATH_CREDIT_RESULT_PARQUET = Constant.HADOOP_OUTPUT + "/parquet/" + now + "/credit"

  /** +++++++++++++++++++schema path+++++++++++++++++++++ */
  val CREDIT_TRADE_VIEW_SCHEMA = "id:String,user_id:String,shop_id:String,gmt_target:String,pay_money:Double"
  val CREDIT_TRADE_SCHEMA = "id:String,user_id:String,shop_id:String,gmt_target:String,pay_money:Double"
  val CREDIT_BETA_SCHEMA = "industry_type:String,sales_upper_limit:Int,sales_lower_limit:Int,score_upper_limit:Int,score_lower_limit:Int,beta_score:Double"
  val CREDIT_PROPERTY_SHOP_SCHEMA = "property_uuid:String,shop_name:String,major_business:String,industry_type:String"
  val CREDIT_REFUND_INFO_SCHEMA = "shop_id:String,refund_rate:Double"
  
  /** ++++++++++++++++ mail  注意以下参数按照实际需求修改++++++++++++++++++++++++++ */
  //以下参数与prop.properties中的参数一致。
  val MAIL_TO = "zhengc@asto-inc.com"
  val MAIL_FROM = "dmp_notice@asto-inc.com"
  val MAIL_PASSWORD = "astodmp2015"
  val MAIL_SMTPHOST = "smtp.qq.com"
  val MAIL_SUBJECT = s"“${PROJECT_NAME}”项目出异常了！"
  val MAIL_CC = ""
  val MAIL_BCC = ""
  val MAIL_TO_FRAUD = "fengtt@asto-inc.com"
  val MAIL_TO_SCORE = "yewb@asto-inc.com"
  val MAIL_TO_APPROVE = "lij@asto-inc.com"
  val MAIL_TO_CREDIT = "lingx@asto-inc.com"
  val MAIL_TO_LOAN_AFTER = "liuw@asto-inc.com"

  //以下参数prop.properties中没有， MAIL_CREDIT_SUBJECT是授信规则模型出问题时邮件的主题
  val MAIL_CREDIT_SUBJECT = s"${PROJECT_NAME}-授信规则 项目出现异常，请尽快查明原因！"
  val MAIL_APPROVE_SUBJECT = s"${PROJECT_NAME}-准入规则结果集写入失败，请尽快查明原因！"

}
