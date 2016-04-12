package scray.querying.source

import scray.querying.queries.DomainQuery
import scray.querying.description.Column
import scray.querying.description.Row
import scray.querying.description.internal.SingleValueDomain
import scray.querying.description.internal.RangeValueDomain
import scalax.collection.immutable.Graph
import scray.querying.description.ColumnOrdering
import scray.querying.caching.Cache
import scray.querying.caching.NullCache
import com.twitter.concurrent.Spool
import scray.querying.description.IndexConfiguration
import scalax.collection.GraphEdge.DiEdge
import scalax.collection.GraphPredef._
import com.twitter.util.Await
import scala.collection.mutable.HashSet
import com.twitter.util.Future
import com.typesafe.scalalogging.slf4j.LazyLogging

//import com.twitter.concurrent.Spool.{seqToSpool, ToSpool}

case class MergeReferenceColumns[Q <: DomainQuery, R, T <: Source[Q, R]](referenceSource: T, referenceSourceColumn: Column, indexConfig: IndexConfiguration, 
    queryTransformer: (Q, IndexConfiguration) => Q = {(a: Q, b: IndexConfiguration) => a})

class IndexMergeSource[Q <: DomainQuery](main: MergeReferenceColumns[Q, Spool[Row], LazySource[Q]], reference: MergeReferenceColumns[Q, Seq[Row], EagerSource[Q]])
    extends LazySource[Q] with LazyLogging {

  override def request(query: Q): LazyDataFuture = {
    logger.debug(s"Merging 2 indexes ${main.referenceSourceColumn.columnName} and ${reference.referenceSourceColumn.columnName}")
   
    val seq = reference.referenceSource.request(reference.queryTransformer(query, reference.indexConfig))      
    main.referenceSource.request(main.queryTransformer (query, main.indexConfig)).flatMap {
      _.flatMap { mainValues =>
        val setOfMainReferences = mainValues.getColumnValue[Set[Any]](main.referenceSourceColumn).map(_.filter { mainValue =>
          Await.result(seq).find {
            y => y.getColumnValue[Set[Any]](reference.referenceSourceColumn).map(_.contains(mainValue)).getOrElse(false)
          }.isDefined
        })
        val fakeRows = setOfMainReferences.map(referenceValue => new FilteredSetRow(mainValues, main.referenceSourceColumn, referenceValue)).toSeq 
        Future.value(fakeRows.toSpool)
      }
    }
  }
  
  class FilteredSetRow[T](row: Row, column: Column, set: Set[T]) extends Row {
    val filteredColumnIndex = row.getColumns.indexOf(column)
    override def getColumnValue[V](colNum: Int): Option[V] = if(filteredColumnIndex > 0 && colNum == filteredColumnIndex) {
        getColumnValue(column)
      } else {
        row.getColumnValue(colNum)
      }
    override def getColumnValue[V](col: Column): Option[V] = if(col == column) {
        Some(set.asInstanceOf[V])
      } else {
        row.getColumnValue[V](col)
      } 
    @inline def intersectValues(cols: HashSet[Column]): Row = row.intersectValues(cols)
    @inline def recalculateInternalCaches: Unit = row.recalculateInternalCaches

    override def getColumns: List[Column] = row.getColumns
    override def getNumberOfEntries: Int = row.getNumberOfEntries 
    override def isEmpty = row.isEmpty
  }
  

  override def isLazy: Boolean = true
  // override def transformSeqElement(element: Row, query: Q): Row = element
  override def getColumns: List[Column] = List(main.referenceSourceColumn)
  override def isOrdered(query: Q): Boolean = true
  override def getDiscriminant = "MergeReferenceColumns" + main.referenceSource.getDiscriminant + reference
  override def createCache: Cache[Nothing] = new NullCache
  override def getGraph: Graph[Source[DomainQuery, Spool[Row]], DiEdge] = (main.referenceSource.getGraph + 
    DiEdge(main.referenceSource.asInstanceOf[Source[DomainQuery, Spool[Row]]],
    this.asInstanceOf[Source[DomainQuery, Spool[Row]]])) ++ 
    (reference.referenceSource.getGraph.asInstanceOf[Graph[Source[DomainQuery, Spool[Row]], DiEdge]] +
    DiEdge(reference.referenceSource.asInstanceOf[Source[DomainQuery, Spool[Row]]],
    this.asInstanceOf[Source[DomainQuery, Spool[Row]]]))
}