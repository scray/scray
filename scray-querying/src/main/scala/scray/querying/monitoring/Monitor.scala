package scray.querying.monitoring

import java.lang.management.ManagementFactory
import scala.collection.JavaConverters._
import scala.collection.mutable.HashMap
import org.mapdb.DBMaker
import org.mapdb.Store
import com.twitter.util.Duration
import com.twitter.util.JavaTimer
import com.typesafe.scalalogging.slf4j.LazyLogging
import javax.management.Attribute
import javax.management.AttributeList
import javax.management.DynamicMBean
import javax.management.MBeanAttributeInfo
import javax.management.MBeanInfo
import javax.management.ObjectName
import scray.querying.Registry
import scray.querying.description.TableConfiguration
import scray.querying.description.TableIdentifier
import scala.util.Random
import scala.collection.mutable.HashMap

private object JMXHelpers {
  implicit def string2objectName(name: String): ObjectName = new ObjectName(name)
  def jmxRegister(ob: Object, obname: ObjectName) =
    ManagementFactory.getPlatformMBeanServer.registerMBean(ob, obname)
}

trait MonitorMBean {
  def getMonitoringInfos(): String
}

class Monitor2 extends DynamicMBean {
  private val strvalues = Array("shock1", "andreas", "johannes", "christian")
  private val intvalues = 1.until(10).toSeq.toArray

  override def getAttribute(attribute: String): Object = {
    if (attribute == "testAttr1") {
      strvalues(Random.nextInt(strvalues.size))
    } else {
      if (attribute == "testAttr2") {
        new Integer(intvalues(Random.nextInt(intvalues.size)))
      } else {
        null
      }
    }
  }

  override def setAttribute(attribute: Attribute): Unit = {}

  override def getMBeanInfo(): MBeanInfo = {
    val att1Info = new MBeanAttributeInfo("testAttr1", "java.lang.String", "Dies ist das ertse Attribut", true, false, false)
    val att2Info = new MBeanAttributeInfo("testAttr2", "java.lang.Integer", "Dies ist das zweite Attribut", true, false, false)
    val attribs = Array[MBeanAttributeInfo](att1Info, att2Info)
    new MBeanInfo(this.getClass.getName, "TestBean for Scray",
      attribs, null, null, null)
  }

  override def setAttributes(attributes: AttributeList): AttributeList =
    getAttributes(attributes.asList.asScala.map(_.getName).toArray)

  override def getAttributes(attributes: Array[String]): AttributeList = {
    val results = new AttributeList
    attributes.foreach(name => results.add(getAttribute(name)))
    results
  }

  override def invoke(actionName: String, params: Array[Object], signature: Array[String]): Object = null
}



class Monitor extends LazyLogging with MonitorMBean {
  import JMXHelpers._
  JMXHelpers.jmxRegister(this, "Johannes:name=Monitor")
  //JMXHelpers.jmxRegister(new Monitor2, "Johannes:00=Johannes2,name=Monitor2")

  var valueXYZZ = "None"
  var namecounter = 0

  private val beans = new HashMap[String, MonitoringInfoBean]

  def getMonitoringInfos(): String = {
    valueXYZZ
  }

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

  def monitor(tables: HashMap[String, HashMap[TableIdentifier, TableConfiguration[_, _, _]]]) {
    logger.debug(s"Monitoring Queryspaces with ${tables.size} entries")

    case class BLA(a: String, b: String)

    def pollCache(name: String): Unit = {
      val timer = new JavaTimer(true)
      timer.schedule(Duration.fromSeconds(3)) {
        println(Registry.getCacheCounter(name))

        val value = Registry.getCacheCounter(name).toString()
        if (!value.startsWith("None")) {
          valueXYZZ = value

          beans.get(name) match {
            case Some(d) =>

            case None =>
              namecounter = namecounter + 1
              val bname = "Johannes:00=Johannes2,name=m" + namecounter.toString()
              val bean = new MonitoringInfoBean(name)
              beans.put(name, bean)
              JMXHelpers.jmxRegister(bean, bname)
          }

        }
      }
    }

    def printTable(tables: HashMap[TableIdentifier, TableConfiguration[_, _, _]]) {
      println(" # = " + tables.size)

      tables.keys.foreach { j =>
        print("Key = " + j + " | ")
        println(tables.get(j))
        pollCache(j.toString)
      }
    }

    if (tables.size > 0) {
      tables.keys.foreach { i =>
        print("Key = " + i)
        println(" # = " + tables.get(i).size)
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