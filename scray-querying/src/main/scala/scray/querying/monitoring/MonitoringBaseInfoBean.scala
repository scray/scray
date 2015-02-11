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
import java.lang.{Integer => JInteger, Boolean => JBoolean}
import javax.management.MBeanOperationInfo
import javax.management.MBeanParameterInfo


class MonitoringBaseInfoBean(monitor: Monitor) extends DynamicMBean {

  private val actionNameID: String = "Request: toggle cache"

  override def getAttribute(attribute: String): Object = {

    if (attribute == "size") {
      new JInteger(monitor.getSize())
    } else {
      if (attribute == "active") {
        new JBoolean(monitor.getCacheActive())
      } else {
        null
      }
    }

  }

  override def setAttribute(attribute: Attribute): Unit = {}

  override def getMBeanInfo(): MBeanInfo = {
    val att1Info = new MBeanAttributeInfo("size", "java.lang.Integer", "Dies ist das ertse Attribut", true, false, false)
    val att2Info = new MBeanAttributeInfo("active", "java.lang.Boolean", "Dies ist das zweite Attribut", true, false, false)
    val attribs = Array[MBeanAttributeInfo](att1Info, att2Info)

    val op1Info = new MBeanOperationInfo(actionNameID, "Toggle cache",
                              null,
                              "Boolean", MBeanOperationInfo.ACTION)
    val ops     = Array[MBeanOperationInfo](op1Info)

    new MBeanInfo(this.getClass.getName, "TestBean for Scray",
      attribs, null, ops, null)
  }

  override def setAttributes(attributes: AttributeList): AttributeList =
    getAttributes(attributes.asList.map(_.getName).toArray)

  override def getAttributes(attributes: Array[String]): AttributeList = {
    val results = new AttributeList
    attributes.foreach(name => results.add(getAttribute(name)))
    results
  }

  override def invoke(actionName: String, params: Array[Object], signature: Array[String]): Object = {
    actionName match {
      case `actionNameID` => Registry.setCachingEnabled(!Registry.getCachingEnabled)
    }
    "Your request has been approved"
  }
}
