package scray.querying.monitoring

import java.lang.{Double => JDouble, Long => JLong, String => JString}
import javax.management.{ AttributeList, DynamicMBean, MBeanInfo, MBeanAttributeInfo,
  Attribute, MBeanOperationInfo, MBeanParameterInfo}
import scala.util.Random
import scala.collection.convert.WrapAsScala._
import com.typesafe.scalalogging.slf4j.LazyLogging
import javax.management.DynamicMBean
import scray.querying.queries.QueryInformation
import scala.collection.mutable.HashMap


class QueryInfoBean (qinfo: QueryInformation, beans : HashMap[String, QueryInfoBean] ) extends DynamicMBean with LazyLogging {

  qinfo.registerDestructionListerner(destructionListerner)

  def destructionListerner() {
   beans.remove(qinfo.qid.toString())
  }

   def getStartTime() : Long = {
     qinfo.startTime
  }

   // if the query succeeds and finishes, this contains the finish time
    def getFinished() : Long = {
     qinfo.finished.get()
  }

  // last time we got updates for this query
   def getPollingTime() : Long = {
     qinfo.pollingTime.get()
  }

   //number of items that we have collected so far
   def getResultItems() : Long = {
     qinfo.resultItems.get()
  }

   //tableId
   def getTableId() : String = {
     qinfo.table.tableId
  }


override def getAttribute(attribute: String): Object = {

  if (attribute == "startTime") {
      new JLong(getStartTime())
    } else {
      if (attribute == "finished") {
        new JLong(getFinished())
      } else {
        if (attribute == "pollingTime") {
          new JLong(getPollingTime())
        } else {
          if (attribute == "resultItems") {
            new JLong(getResultItems())
          } else {
          if (attribute == "tableId") {
            new JString(getTableId())
          }
          else {
            null
          }
          }
        }
      }
    }

}

override def setAttribute(attribute: Attribute): Unit = {}

val att1Info = new MBeanAttributeInfo("startTime", "long", "Attribut", true, false, false)
val att2Info = new MBeanAttributeInfo("finished", "long", "Attribut", true, false, false)
val att3Info = new MBeanAttributeInfo("pollingTime", "long", "Attribut", true, false, false)
val att4Info = new MBeanAttributeInfo("resultItems", "long", "Attribut", true, false, false)
val att5Info = new MBeanAttributeInfo("tableId", "String", "Attribut", true, false, false)
val attribs = Array[MBeanAttributeInfo](att1Info, att2Info, att3Info, att4Info, att5Info)

val ops = null

val info = new MBeanInfo(this.getClass.getName, "QueryBean for Scray",
      attribs, null, ops, null)

override def getMBeanInfo(): MBeanInfo = info

  override def setAttributes(attributes: AttributeList): AttributeList =
    getAttributes(attributes.asList.map(_.getName).toArray)

  override def getAttributes(attributes: Array[String]): AttributeList = {
    val results = new AttributeList
    attributes.foreach(name => results.add(getAttribute(name)))
    results
  }

override def invoke(actionName: String, params: Array[Object], signature: Array[String]): Object = null

}