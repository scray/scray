// See the LICENCE.txt file distributed with this work for additional
// information regarding copyright ownership.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.
package scray.querying.monitoring

import java.lang.management.ManagementFactory
import scala.collection.JavaConverters._
import scala.collection.mutable.HashMap
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
import com.typesafe.scalalogging.LazyLogging
import scray.querying.queries.DomainQuery

/**
 * register a new MBean with JMX
 */
private object JMXHelpers {
  implicit def string2objectName(name: String): ObjectName = new ObjectName(name)
  def jmxRegister(ob: Object, obname: ObjectName): Unit =
    ManagementFactory.getPlatformMBeanServer.registerMBean(ob, obname)
  def jmxUnregister(obname: ObjectName): Unit =
    ManagementFactory.getPlatformMBeanServer.unregisterMBean(obname)
}

/**
 * organizes jmx beans
 */
class Monitor extends LazyLogging {
  import JMXHelpers._

  private val beans = new HashMap[String, MonitoringInfoBean]

  private val lock = new ReentrantLock

  def getSize(): Int = {
    lock.lock()
    try {
      beans.size
    } finally {
      lock.unlock()
    }
  }

  def getCacheActive(): Boolean = Registry.getCachingEnabled

  JMXHelpers.jmxRegister(new MonitoringBaseInfoBean(this), "Scray:name=Cache")

  /**
   * monitor caches and queries
   */
  def monitor(tables: HashMap[String, HashMap[TableIdentifier, TableConfiguration[_ <: DomainQuery, _ <: DomainQuery, _]]]) {
    logger.debug(s"Monitoring Queryspaces with ${tables.size} entries")
    val timer = new JavaTimer(true)
    timer.schedule(Duration.fromSeconds(3)) {

      // setup polling for a cache identified by its discriminant
      def pollCache(name: TableIdentifier): Unit = {
        lock.lock()
        try {
          Registry.getCacheCounter(name.toString) match {
            case None ⇒ beans.get(name.toString) match {
              case None ⇒
                val bname = s"Scray:00=Tables,name=${name.tableId}_${name.dbId}_${name.dbSystem}"
                val bean = new MonitoringInfoBean(name.toString)
                beans.put(name.toString, bean)
                JMXHelpers.jmxRegister(bean, bname)
              case _ ⇒
            }
            case _ ⇒
          }
        } finally {
          lock.unlock()
        }
      }

      // walk over all table identifiers to retrieve a key for the caches
      def walkTables(tablesInSpace: HashMap[TableIdentifier, TableConfiguration[_ <: DomainQuery, _ <: DomainQuery, _]]): Unit =
        tablesInSpace.keys.foreach(pollCache(_))

      if (tables.size > 0) {
        // walk over all query spaces and extract table information
        tables.keys.foreach(i ⇒ walkTables(tables.get(i).get))
      }
    }
  }

}
