package scray.querying.source

import scray.querying.description.Row
import scray.querying.queries.DomainQuery
import scray.querying.description.TableIdentifier
import scray.querying.description.Column
import scala.reflect.ClassTag
import scray.querying.description.ColumnOrdering
import scray.querying.description.QueryRange
import scray.querying.description.internal.Domain
import scray.querying.description.ColumnGrouping
import com.typesafe.scalalogging.slf4j.LazyLogging

/**
 * a hash join source for indexes that use ranges and sets of references
 */
abstract class AbstractRangeSetHashJoinSource[Q <: DomainQuery, M, R /* <: Product */, V](
    indexsource: LazySource[Q],
    lookupSource: KeyValueSource[R, V],
    lookupSourceTable: TableIdentifier,
    lookupkeymapper: Option[M => R] = None,
    maxLimit: Option[Long] = None)(implicit tag: ClassTag[M]) 
  extends AbstractHashJoinSource[Q, M, R, V](indexsource, lookupSource, lookupSourceTable, lookupkeymapper)
  with LazyLogging {

  /**
   * the name of the column in lookupsource that will be the primary 
   * key that is used as reference indexed
   */
  @inline protected def getReferenceLookupSourceColumn: Column

  /**
   * the column in the index that contains a set of references into lookupsource
   */
  @inline protected def getReferencesIndexColumn: Column
  
  /**
   * the column in the idnex that contains the value of the index data 
   */
  @inline protected def getValueIndexColumn: Column
  
  /**
   * a primary column in the index used as a criteria shrinking 
   * the number of possible results
   */
  @inline protected def getPrefixIndexColumn: Column

  
  /**
   * creates a domain query that matches the provided domains and trys to reflect
   * the original query options
   */
  @inline protected def createDomainQuery[T](query: Q, domains: List[Domain[_]])(implicit ord: Ordering[T]): Q = {
    val resultColumns = Set(getPrefixIndexColumn,
        getValueIndexColumn, getReferencesIndexColumn)
    val range = query.getQueryRange.map { qrange => 
      val skipLines = qrange.skip.getOrElse(0L)
      QueryRange(None, qrange.limit.map(_ + skipLines).orElse(maxLimit.map(_ + skipLines)))
    }
    DomainQuery(query.getQueryID, query.getQueryspace, query.querySpaceVersion, resultColumns, getPrefixIndexColumn.table,
        domains, Some(ColumnGrouping(getValueIndexColumn)),
        Some(ColumnOrdering[T](getValueIndexColumn,
                query.getOrdering.filter(_.descending).isDefined)), range).asInstanceOf[Q]
  }  
  
  /**
   * we return an array of references we want to look up
   */
  override protected def getJoinablesFromIndexSource(index: Row): Array[M] = {
    index.getColumnValue[M](getReferencesIndexColumn) match {
      case Some(refs) => refs match {
        case travs: TraversableOnce[M] => travs.asInstanceOf[TraversableOnce[M]].toArray
        case travs: M => Array[M](travs)
      }
      case None => Array[M]()
    }
  }
  
  override protected def isOrderedAccordingToOrignalOrdering(transformedQuery: Q, ordering: ColumnOrdering[_]): Boolean =
    ordering.column == getReferenceLookupSourceColumn
  
  /**
   * since this is a true index only, we only return the referred columns  
   */
  override def getColumns: Set[Column] = lookupSource.getColumns
}