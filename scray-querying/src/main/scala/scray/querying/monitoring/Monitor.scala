package scray.querying.monitoring

import java.lang.management.ManagementFactory

import scala.collection.JavaConverters._
import scala.collection.mutable.HashMap
import scala.collection.mutable.HashMap

import org.mapdb.DBMaker
import org.mapdb.Store

import com.twitter.util.Duration
import com.twitter.util.JavaTimer
import com.typesafe.scalalogging.slf4j.LazyLogging

import javax.management.ObjectName
import scray.querying.Registry
import scray.querying.description.TableConfiguration
import scray.querying.description.TableIdentifier

private object JMXHelpers {
  implicit def string2objectName(name: String): ObjectName = new ObjectName(name)
  def jmxRegister(ob: Object, obname: ObjectName) =
    ManagementFactory.getPlatformMBeanServer.registerMBean(ob, obname)
}

trait MonitorMBean {
  def getSize(): Int
  def getCacheActive(): Boolean
}

class Monitor extends LazyLogging with MonitorMBean {
  import JMXHelpers._
  JMXHelpers.jmxRegister(new MonitoringBaseInfoBean(this), "Scray:name=Cache")
  //JMXHelpers.jmxRegister(new Monitor2, "Johannes:00=Johannes2,name=Monitor2")

  var valueXYZZ = "None"
  var namecounter = 0

  private val beans = new HashMap[String, MonitoringInfoBean]

  def getSize(): Int = {
    beans.size
  }

  def getCacheActive(): Boolean = Registry.getCachingEnabled

  def testCache() {
    val db = DBMaker.newMemoryDirectDB().transactionDisable().asyncWriteEnable().make
    val cache = db.createHashMap("cache").counterEnable().expireStoreSize(1.0d).make[Int, String]
    val key = 42;
    val value = "test";
    val db1 = DBMaker.newMemoryDirectDB().transactionDisable().asyncWriteEnable().make
    val cache1 = db1.createHashMap("cache").counterEnable().expireStoreSize(1.0d).make[Int, String]
    val db2 = DBMaker.newMemoryDirectDB().transactionDisable().asyncWriteEnable().make
    val cache2 = db2.createHashMap("cache").counterEnable().expireStoreSize(1.0d).make[Int, String]
    val db3 = DBMaker.newMemoryDirectDB().transactionDisable().asyncWriteEnable().make
    val cache3 = db3.createHashMap("cache").counterEnable().expireStoreSize(1.0d).make[Int, String]

    cache.put(key, value);
    cache2.put(key, "ansders");

    println(cache.get(key))
    println(cache1.get(key))
    println(cache2.get(key))
    println(cache3.get(key))
  }

  def monitor() {}

  def monitor(a: String) {

  }

  private def deStringfyName(name: String): String =
    name.stripPrefix("TableIdentifier(").stripSuffix(")").split(",").mkString("_")

  def monitor(tables: HashMap[String, HashMap[TableIdentifier, TableConfiguration[_, _, _]]]) {
    logger.debug(s"Monitoring Queryspaces with ${tables.size} entries")

    case class BLA(a: String, b: String)

    def pollCache(name: String): Unit = {
      val timer = new JavaTimer(true)
      timer.schedule(Duration.fromSeconds(3)) {
        //println(Registry.getCacheCounter(name))

        val value = Registry.getCacheCounter(name).toString()
        if (!value.startsWith("None")) {
          valueXYZZ = value

          beans.get(name) match {
            case Some(d) =>

            case None =>
              //namecounter = namecounter + 1
              val bname = "Scray:00=Tables,name=" + deStringfyName(name)
              val bean = new MonitoringInfoBean(name)
              beans.put(name, bean)
              JMXHelpers.jmxRegister(bean, bname)
          }

        }
      }
    }

    def printTable(tables: HashMap[TableIdentifier, TableConfiguration[_, _, _]]) {
      //println(" # = " + tables.size)

      tables.keys.foreach { j =>
        /* print("Key = " + j + " | ")
        println(tables.get(j)) */
        pollCache(j.toString)
      }
    }

    if (tables.size > 0) {
      tables.keys.foreach { i =>
        //print("Key = " + i)
        //println(" # = " + tables.get(i).size)
        printTable(tables.get(i).get)
      }
    }

  }
}

object Monitor {

  val db = DBMaker.newMemoryDirectDB().transactionDisable().make
  val cache = db.createHashMap("cache").counterEnable().expireStoreSize(1.0d).make[String, String]
  val store = Store.forDB(db)

  def printCacheSize() = {
    println(s"Sizes curr: ${store.getCurrSize} free: ${store.getFreeSize} cacheSize: ${cache.size()}")
  }

  def main(args: Array[String]): Unit = {
    1.until(1000).foreach { i => cache.put(s"$i", s"$i + 1"); printCacheSize }
    cache.close
  }
}