package scray.querying.monitoring

import java.lang.management.ManagementFactory
import scala.collection.JavaConverters._
import scala.collection.mutable.HashMap
import org.mapdb.DBMaker
import org.mapdb.Store
import com.twitter.util.Duration
import com.twitter.util.JavaTimer
import javax.management.ObjectName
import scray.querying.Registry
import scray.querying.description.TableConfiguration
import scray.querying.description.TableIdentifier
import java.util.concurrent.locks.ReentrantLock
import scray.querying.queries.QueryInformation
import com.typesafe.scalalogging.LazyLogging


class MonitorQuery extends LazyLogging {

  import JMXHelpers._

  private val beans = new HashMap[String, QueryInfoBean]

  private val lock = new ReentrantLock

  def getSize(): Int = {
    lock.lock()
    try {
      beans.size
    } finally {
      lock.unlock()
    }
  }

  def queryInformationListener(qinfo: QueryInformation) {
    val bname = s"Scray:00=Queries,name=${qinfo.qid.toString()}"
    val bean = new QueryInfoBean(qinfo, beans)
    beans.put(qinfo.qid.toString, bean)
    JMXHelpers.jmxRegister(bean, bname)
  }

  Registry.addCreateQueryInformationListener { x => queryInformationListener(x) }

  //Map[UUID, QueryInformation]
  /* private val qinfo = Registry.getQueryInformations()

  for ((uuid,info) <- qinfo) {
    val bname = s"Scray:00=Queries,name=${uuid.toString()}"
    val bean = new QueryInfoBean(uuid.toString())
    beans.put(uuid.toString, bean)
    JMXHelpers.jmxRegister(bean, bname)} */

}