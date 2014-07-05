package com.datastax.driver.spark.connector

import java.lang.reflect.{Proxy, Method, InvocationHandler}
import com.datastax.driver.core.{RegularStatement, SimpleStatement, Session}

/** Wraps a `Session` and intercepts:
  *  - `close` method to invoke `afterClose` handler
  *  - `prepare` methods to cache `PreparedStatement` objects. */
class SessionProxy(session: Session, afterClose: Session => Any) extends InvocationHandler {

  private var closed = false

  private def getQuery(firstArgType: Class[_], firstArg: AnyRef): RegularStatement = {
    if (firstArgType == classOf[String])
      new SimpleStatement(firstArg.asInstanceOf[String])
    else
      firstArg.asInstanceOf[RegularStatement]
  }

  override def invoke(proxy: Any, method: Method, args: Array[AnyRef]) = {
    try {
      if (method.getName == "prepare") {
        val firstArgType = method.getParameterTypes()(0)
        val firstArg = args(0)
        val query = getQuery(firstArgType, firstArg)
        PreparedStatementCache.prepareStatement(session, query)
      }
      else
        method.invoke(session, args: _*)
    }
    finally {
      if (method.getName == "close" && !closed) {
        closed = true
        afterClose(session)
      }
    }
  }
}

object SessionProxy {

  /** Creates a new `SessionProxy` delegating to the given `Session`.
    * The proxy adds prepared statement caching functionality. */
  def wrap(session: Session): Session =
    wrapWithCloseAction(session)(_ => ())

  /** Creates a new `SessionProxy` delegating to the given `Session`.
    * Additionally registers a callback on `Session#close` method.
    * @param afterClose code to be invoked after the session has been closed */
  def wrapWithCloseAction(session: Session)(afterClose: Session => Any): Session =
    Proxy.newProxyInstance(
      session.getClass.getClassLoader,
      Array(classOf[Session]),
      new SessionProxy(session, afterClose)).asInstanceOf[Session]
}