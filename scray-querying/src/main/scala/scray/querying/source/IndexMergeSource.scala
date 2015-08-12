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
import com.typesafe.scalalogging.slf4j.LazyLogging
import com.twitter.concurrent.Spool
import scray.querying.description.IndexConfiguration
import scalax.collection.GraphEdge.DiEdge
import scalax.collection.GraphPredef._
import com.twitter.util.Await

case class MergeReferenceColumns[Q <: DomainQuery, R, T <: Source[Q, R]](referenceSource: T, referenceSourceColumn: Column, indexConfig: IndexConfiguration, 
    queryTransformer: (Q, IndexConfiguration) => Q = {(a: Q, b: IndexConfiguration) => a})

class IndexMergeSource[Q <: DomainQuery](main: MergeReferenceColumns[Q, Spool[Row], LazySource[Q]], reference: MergeReferenceColumns[Q, Seq[Row], EagerSource[Q]])
    extends LazySource[Q] with LazyLogging {
  
  override def request(query: Q): LazyDataFuture = {
    val seq = reference.referenceSource.request(reference.queryTransformer(query, reference.indexConfig))
    main.referenceSource.request(main.queryTransformer(query, main.indexConfig)).filter { mainValue => 
      Await.result(seq).find {
          y => y.getColumnValue(reference.referenceSourceColumn) == (mainValue.head.getColumnValue(main.referenceSourceColumn))
      }.isDefined
     }
  }
  

  override def isLazy: Boolean = true
  //override def transformSeqElement(element: Row, query: Q): Row = element
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
