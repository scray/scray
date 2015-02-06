package scray.querying.monitoring

import javax.management.DynamicMBean
import javax.management.MBeanInfo
import javax.management.MBeanAttributeInfo
import javax.management.Attribute
import scala.util.Random
import javax.management.AttributeList
import scala.collection.convert.WrapAsScala._
import scray.querying.Registry
import scray.querying.caching.MonitoringInfos
import java.lang.{Double => JDouble, Long => JLong}


class MonitoringInfoBean(name: String) extends DynamicMBean {

  def getValueSizeGB() : Double = {
    val cacheCounter = Registry.getCacheCounter(name)
    if (!cacheCounter.toString().startsWith("None"))
      cacheCounter.get.sizeGB
    else 0.0d
  }

  def getEntries() : Long = {
    val cacheCounter = Registry.getCacheCounter(name)
    if (!cacheCounter.toString().startsWith("None"))
      cacheCounter.get.entries
    else 0L
  }

  def getCurrentSize() : Long = {
    val cacheCounter = Registry.getCacheCounter(name)
    if (!cacheCounter.toString().startsWith("None"))
      cacheCounter.get.currentSize
    else 0L
  }

  def getFreeSize(): Long = {
    val cacheCounter = Registry.getCacheCounter(name)
    if (!cacheCounter.toString().startsWith("None"))
      cacheCounter.get.freeSize
    else 0L
  }


  override def getAttribute(attribute: String): Object = {
    if (attribute == "sizeGB") {
      new JDouble(getValueSizeGB())
    } else {
      if (attribute == "entries") {
        new JLong(getEntries())
      } else {
        if (attribute == "currentSize") {
          new JLong(getCurrentSize())
        } else {
          if (attribute == "freeSize") {
            new JLong(getFreeSize())
          } else {
            null
          }
        }
      }
    }
  }

  override def setAttribute(attribute: Attribute): Unit = {}

  override def getMBeanInfo(): MBeanInfo = {
    val att1Info = new MBeanAttributeInfo("sizeGB", "java.lang.Double", "Dies ist das ertse Attribut", true, false, false)
    val att2Info = new MBeanAttributeInfo("entries", "java.lang.Long", "Dies ist das zweite Attribut", true, false, false)
    val att3Info = new MBeanAttributeInfo("currentSize", "java.lang.Long", "Dies ist das ertse Attribut", true, false, false)
    val att4Info = new MBeanAttributeInfo("freeSize", "java.lang.Long", "Dies ist das zweite Attribut", true, false, false)
    val attribs = Array[MBeanAttributeInfo](att1Info, att2Info, att3Info, att4Info)
    new MBeanInfo(this.getClass.getName, "TestBean for Scray",
      attribs, null, null, null)
  }

  override def setAttributes(attributes: AttributeList): AttributeList =
    getAttributes(attributes.asList.map(_.getName).toArray)

  override def getAttributes(attributes: Array[String]): AttributeList = {
    val results = new AttributeList
    attributes.foreach(name => results.add(getAttribute(name)))
    results
  }

  override def invoke(actionName: String, params: Array[Object], signature: Array[String]): Object = null
}
