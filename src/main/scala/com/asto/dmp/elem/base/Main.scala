package com.asto.dmp.elem.base

import com.asto.dmp.elem.service._
import com.asto.dmp.elem.util.logging.Logging

object Main extends Logging {
  def main(args: Array[String]) {
    //初始化SparkContext,注意“local”在部署到集群上时需要去掉，local适用于IDEA上运行！
    BaseContext.initSparkContext("local")

    if ("001" == args(0)) {
      //准入模型
      new ApproveService().run
    } else if ("002" == args(0)) {
      //反欺诈模型
      new AntiFraudService().run
    } else if ("003" == args(0)) {
      //评分模型
      new ScoreService().run
    } else if ("004" == args(0)) {
      //授信模型
      new CreditService().run
    } else if ("005"== args(0)) {
      //贷后模型
      new LoanAfterService().run
    } else {
      error(s"传入参数错误!传入的是${args(0)},请传入001~005")
    }

    BaseContext.stopSparkContext()
    System.exit(0)
  }
}
