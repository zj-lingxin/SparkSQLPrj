package com.asto.dmp.elem.dao

import com.asto.dmp.elem.base.DataSource

/**
 * Created by lingx on 2015/10/14.
 */
class BizDao extends DataSource {
  def orderProperty(properties: String = "*") = {
    if (properties == "*") {

    } else {

    }
    sqlContext.sql(s"select $properties from order ").map(a => (a(0).toString, a(1)))
  }
  private def foo() = {

  }
}
