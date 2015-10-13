package com.asto.dmp.elem.base

import com.asto.dmp.elem.service._
import com.asto.dmp.elem.util.logging.Logging

object Main extends Logging {
  def main(args: Array[String]) {
    if (args == null || args.length == 0) {
      logError("请传入模型编号：001~005")
      return
    }
    //现在下面这句可以删掉了，它会默认设置成local[2]。可以接收集群配置文件传来的Master值或者是submit后面的--master。
    //BaseContext.initSparkContext("local[2]")
    args(0) match {
      case "001" =>
        //准入模型
        logInfo(s"开始运行准入模型(001)")
        new ApproveService().run()
      case "002" =>
        //反欺诈模型
        logInfo(s"开始运行反欺诈模型(002)")
        new AntiFraudService().run()
      case "003" =>
        //评分模型
        logInfo(s"开始运行评分模型(003)")
        new ScoreService().run()
      case "004" =>
        //授信模型
        logInfo(s"开始运行授信模型(004)")
        new CreditService().run()
      case "005" =>
        //贷后模型
        logInfo(s"开始运行贷后模型(005)")
        new LoanAfterService().run()
      case _     =>
        logError(s"传入参数错误!传入的是${args(0)},请传入001~005")
    }

    BaseContext.stopSparkContext()
    System.exit(0)
  }
}
