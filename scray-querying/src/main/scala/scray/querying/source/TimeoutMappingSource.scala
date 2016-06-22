package scray.querying.source

import com.typesafe.scalalogging.slf4j.LazyLogging
import scray.querying.description.Row
import scray.querying.queries.DomainQuery
import scray.querying.description.EmptyRow
import scray.querying.description.internal.SingleValueDomain
import scray.querying.description.internal.QueryTimeOutException


/**
 * Check if query timed out. Triggered by received data.
 */
class TimeoutMappingSource [Q <: DomainQuery](source: LazySource[Q]) extends LazyQueryMappingSource[Q](source) with LazyLogging {
  
  var startTime: Long = 0;
  
  override def transformSpoolElement(element: Row, query: Q): Row = {
    query.getQueryRange.filter(queryRange => queryRange.timeout  match {
      case None =>   false
      case Some(time) => {(time * 1000 + startTime) < System.currentTimeMillis()}
    }) match {
      case None => element
      case Some(x) => throw  new QueryTimeOutException(query)
    }
  }
  
  override def init(query: Q): Unit = {
    startTime = System.currentTimeMillis();
  }
  
  def getColumns: List[scray.querying.description.Column] = { source.getColumns }
  def getDiscriminant: String = {"Timeout" + source.getDiscriminant}
}