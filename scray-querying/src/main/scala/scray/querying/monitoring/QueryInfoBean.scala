package scray.querying.monitoring

import java.lang.{ Long => JLong }
import java.lang.{ String => JString }

import scala.collection.convert.WrapAsScala._
import scala.collection.mutable.HashMap

import com.typesafe.scalalogging.slf4j.LazyLogging

import javax.management.Attribute
import javax.management.AttributeList
import javax.management.DynamicMBean
import javax.management.DynamicMBean
import javax.management.MBeanAttributeInfo
import javax.management.MBeanInfo
import scray.querying.description.And
import scray.querying.description.AtomicClause
import scray.querying.description.Clause
import scray.querying.description.Equal
import scray.querying.description.Greater
import scray.querying.description.GreaterEqual
import scray.querying.description.IsNull
import scray.querying.description.Or
import scray.querying.description.Smaller
import scray.querying.description.SmallerEqual
import scray.querying.description.Unequal
import scray.querying.description.Wildcard
import scray.querying.queries.QueryInformation

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
  
  def getFinishedPlanningTime(): Long = {
    qinfo.finishedPlanningTime.get()
  }
  
  def getRequestSentTime(): Long = {
    qinfo.requestSentTime.get
  }

  private def handleSubClauses(buffer: StringBuilder, clauses: List[Clause], combiner: String): StringBuilder = {
    clauses.foldLeft(0) { (count, cl) => 
      if(count > 0) { 
        buffer ++= combiner
      }
      buffer ++= " ( "
      recurseQueryFilters(buffer, cl)
      buffer ++= " ) "
      count + 1
    }
    buffer
  }
  
  def recurseQueryFilters(buffer: StringBuilder, clause: Clause): StringBuilder = clause match {
    case c: Equal[_] =>  buffer ++= c.column.columnName + "=" + c.value.toString()
    case c: Greater[_] => buffer ++= c.column.columnName + ">" + c.value.toString()
    case c: GreaterEqual[_] => buffer ++= c.column.columnName + ">=" + c.value.toString()
    case c: Smaller[_] => buffer ++= c.column.columnName + "<" + c.value.toString()
    case c: SmallerEqual[_] => buffer ++= c.column.columnName + "<=" + c.value.toString()
    case c: Unequal[_] => buffer ++= c.column.columnName + "<>" + c.value.toString()
    case c: IsNull[_] => buffer ++= c.column.columnName + "is null"
    case c: Wildcard[_] => buffer ++= c.column.columnName + "LIKE" + c.value.toString()
    case c: Or => handleSubClauses(buffer, c.clauses.toList, "OR")
    case c: And => handleSubClauses(buffer, c.clauses.toList, "AND")
  }
  //filters
  def getFilters(): String = {
    qinfo.where.map { clause =>
      recurseQueryFilters(new StringBuilder, clause).toString
    }.getOrElse("")
  }

  override def getAttribute(attribute: String): Object = attribute match {
    case "startTime" => new JLong(getStartTime())
    case "finished" => new JLong(getFinished())
    case "pollingTime" => new JLong(getPollingTime())
    case "resultItems" => new JLong(getResultItems())
    case "filters" => new JString(getFilters())
    case "tableId" => new JString(getTableId())
    case "finishedPlanningTime" => new JLong(getFinishedPlanningTime())
    case "requestSentTime" => new JLong(getFinishedPlanningTime())
    case _ => null
  }

  override def setAttribute(attribute: Attribute): Unit = {}

  val att1Info = new MBeanAttributeInfo("startTime", "long", "Attribut", true, false, false)
  val att2Info = new MBeanAttributeInfo("finished", "long", "Attribut", true, false, false)
  val att3Info = new MBeanAttributeInfo("pollingTime", "long", "Attribut", true, false, false)
  val att4Info = new MBeanAttributeInfo("resultItems", "long", "Attribut", true, false, false)
  val att5Info = new MBeanAttributeInfo("filters", "String", "Attribut", true, false, false)
  val att6Info = new MBeanAttributeInfo("tableId", "String", "Attribut", true, false, false)
  val att7Info = new MBeanAttributeInfo("finishedPlanningTime", "long", "Attribut", true, false, false)
  val att8Info = new MBeanAttributeInfo("requestSentTime", "long", "Attribut", true, false, false)
  val attribs = Array[MBeanAttributeInfo](att1Info, att2Info, att3Info, att4Info, att5Info, att6Info, att7Info, att8Info)

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