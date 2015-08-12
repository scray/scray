package scray.querying.monitoring

import java.lang.{ Long => JLong }
import java.lang.{ String => JString }
import scala.collection.convert.WrapAsScala._
import scala.collection.mutable.HashMap
import com.typesafe.scalalogging.slf4j.LazyLogging
import javax.management.DynamicMBean
import javax.management.DynamicMBean
import scray.querying.description.AtomicClause
import scray.querying.description.Clause
import scray.querying.queries.QueryInformation
import javax.management.MBeanInfo
import scray.querying.description.Smaller
import scray.querying.description.GreaterEqual
import scray.querying.description.SmallerEqual
import scray.querying.description.Unequal
import javax.management.MBeanAttributeInfo
import scray.querying.description.Equal
import scray.querying.description.Greater
import scray.querying.description.And
import scray.querying.description.IsNull
import javax.management.AttributeList
import javax.management.Attribute
import scray.querying.description.Or

class QueryInfoBean(qinfo: QueryInformation, beans: HashMap[String, QueryInfoBean]) extends DynamicMBean with LazyLogging {

  qinfo.registerDestructionListerner(destructionListerner)

  def destructionListerner() {
    beans.remove(qinfo.qid.toString())
  }

  def getStartTime(): Long = {
    qinfo.startTime
  }

  // if the query succeeds and finishes, this contains the finish time
  def getFinished(): Long = {
    qinfo.finished.get()
  }

  // last time we got updates for this query
  def getPollingTime(): Long = {
    qinfo.pollingTime.get()
  }

  //number of items that we have collected so far
  def getResultItems(): Long = {
    qinfo.resultItems.get()
  }

  //tableId
  def getTableId(): String = {
    qinfo.table.tableId
  }

  def recurseQueryFilters(clause: Clause, acc: List[(String, String)]): List[(String, String)] = clause match {
    case c: Equal[_] => acc :+ (c.column.columnName, "=")
    case c: Greater[_] => acc :+ (c.column.columnName, ">")
    case c: GreaterEqual[_] => acc :+ (c.column.columnName, ">=")
    case c: Smaller[_] => acc :+ (c.column.columnName, "<")
    case c: SmallerEqual[_] => acc :+ (c.column.columnName, "<=")
    case c: Unequal[_] => acc :+ (c.column.columnName, "<>")
    case c: IsNull[_] => acc :+ (c.column.columnName, "is null")
    case c: Or => c.clauses.flatMap(cl => recurseQueryFilters(cl, List())).toList
    case c: And => c.clauses.flatMap(cl => recurseQueryFilters(cl, List())).toList
  }

  //filters
  def getFilters(): String = {
    qinfo.where.map { clause =>
      val sorted = recurseQueryFilters(clause, List()).sortWith((a, b) => a._1 < b._1)
      sorted.toString()
    }.getOrElse("")
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
            if (attribute == "filters") {
              new JString(getFilters())
            } else {
              if (attribute == "tableId") {
                new JString(getTableId())
              } else {
                null
              }
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
  val att5Info = new MBeanAttributeInfo("filters", "String", "Attribut", true, false, false)
  val att6Info = new MBeanAttributeInfo("tableId", "String", "Attribut", true, false, false)
  val attribs = Array[MBeanAttributeInfo](att1Info, att2Info, att3Info, att4Info, att5Info, att6Info )

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