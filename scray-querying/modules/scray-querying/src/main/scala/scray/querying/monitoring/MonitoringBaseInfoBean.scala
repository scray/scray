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
import scala.collection.mutable.HashMap


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

  val att1Info = new MBeanAttributeInfo("size", "int", "Attribut", true, false, false)
  val att2Info = new MBeanAttributeInfo("active", "boolean", "Attribut", true, false, false)
  val attribs = Array[MBeanAttributeInfo](att1Info, att2Info)

  val op1Info = new MBeanOperationInfo(actionNameID, "Toggle cache",
                              null,
                              "Boolean", MBeanOperationInfo.ACTION)
  val ops     = Array[MBeanOperationInfo](op1Info)

  val info = new MBeanInfo(this.getClass.getName, "TestBean for Scray",
      attribs, null, ops, null)


  override def getMBeanInfo(): MBeanInfo = info

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
