package com.asto.dmp.elem.base

import org.apache.spark.sql.types._

trait DataSource extends scala.Serializable {
  protected val sc = BaseContext.getSparkContext()
  //使用lazy,当确实要使用HiveContext或者SqlContext时再去初始化
  protected lazy val sqlContext = BaseContext.getSqlContext()
  protected lazy val hiveContext = BaseContext.getHiveContext()

  def getSchema(schemaStr: String): StructType = {
    StructType(schemaStr.split(",").map(fieldName => StructField(fieldName.split(":")(0).trim,
      fieldName.split(":")(1).trim match {
        case "Double" => DoubleType
        case "String" => StringType
        case "Long" => LongType
        case "Int" => IntegerType
        case "Integer" => IntegerType
        case "Float" => FloatType
      }
      , true)))
  }
}
