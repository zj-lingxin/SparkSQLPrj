package com.asto.dmp.elem.util.logging

import com.asto.dmp.elem.base.Constant
import org.apache.log4j.PropertyConfigurator
import org.slf4j.impl.StaticLoggerBinder
import org.slf4j.{Logger, LoggerFactory}

trait Logging {

  @transient private var _log: Logger = null

  protected def logName = {
    this.getClass.getName.stripSuffix("$")
  }

  protected def log: Logger = {
    if (_log == null) {
      initializeIfNecessary()
      _log = LoggerFactory.getLogger(logName)
    }
    _log
  }

  protected def info(msg: => String) {
    if (log.isInfoEnabled) log.info(msg)
  }

  protected def debug(msg: => String) {
    if (log.isDebugEnabled) log.debug(msg)
  }

  protected def trace(msg: => String) {
    if (log.isTraceEnabled) log.trace(msg)
  }

  protected def warn(msg: => String) {
    if (log.isWarnEnabled) log.warn(msg)
  }

  protected def error(msg: => String) {
    if (log.isErrorEnabled) log.error(msg)
  }

  protected def info(msg: => String, throwable: Throwable) {
    if (log.isInfoEnabled) log.info(msg, throwable)
  }

  protected def debug(msg: => String, throwable: Throwable) {
    if (log.isDebugEnabled) log.debug(msg, throwable)
  }

  protected def trace(msg: => String, throwable: Throwable) {
    if (log.isTraceEnabled) log.trace(msg, throwable)
  }

  protected def warn(msg: => String, throwable: Throwable) {
    if (log.isWarnEnabled) log.warn(msg, throwable)
  }

  protected def error(msg: => String, throwable: Throwable) {
    if (log.isErrorEnabled) log.error(msg, throwable)
  }

  protected def isTraceEnabled(): Boolean = {
    log.isTraceEnabled
  }

  private def initializeIfNecessary() {
    if (!Logging._initialized) {
      Logging.initLock.synchronized {
        if (!Logging._initialized) {
          initializeLogging()
        }
      }
    }
  }

  private def initializeLogging() {
    val binderClass = StaticLoggerBinder.getSingleton.getLoggerFactoryClassStr

    val usingLog4j12 = "org.slf4j.impl.Log4jLoggerFactory".equals(binderClass)
    if (usingLog4j12) {
      val defaultLogProps = Constant.DEFAULT_LOG_PROPS
      Option(getClass.getClassLoader.getResource(defaultLogProps)) match {
        case Some(url) =>
          PropertyConfigurator.configure(url)
          System.err.println(s"Using project's log4j profile: $defaultLogProps")
        case None =>
          System.err.println(s"project was unable to load $defaultLogProps")
      }
    }
    Logging._initialized = true
    log
  }
}

private object Logging {
  @volatile private var _initialized = false
  val initLock = new Object()
  try {
    val bridgeClass = Class.forName("org.slf4j.bridge.SLF4JBridgeHandler")
    bridgeClass.getMethod("removeHandlersForRootLogger").invoke(null)
    val installed = bridgeClass.getMethod("isInstalled").invoke(null).asInstanceOf[Boolean]
    if (!installed) {
      bridgeClass.getMethod("install").invoke(null)
    }
  } catch {
    case e: ClassNotFoundException => // can't log anything yet so just fail silently
  }
}
