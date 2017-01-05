package scray.querying.planning

import scray.querying.description.QueryspaceConfiguration
import scray.querying.description.internal.MaterializedView
import scray.querying.queries.DomainQuery
import scray.querying.description.ColumnConfiguration
import scray.querying.description.TableConfiguration
import scray.querying.description.Column
import scray.querying.description.ColumnConfiguration
import scray.querying.description.TableConfiguration
import scray.querying.description.TableIdentifier
import scray.querying.description.Row
import com.twitter.util.Future
import scray.querying.description.SimpleRow
import scray.querying.source.DomainFilterSource
import scray.querying.source.QueryableSource
import scray.querying.source.QueryableSource

class TestQuerySpace(tables: Set[TableConfiguration[_ <: DomainQuery, _ <: DomainQuery, _]]) extends QueryspaceConfiguration("test") {
  
  def getColumns(version: Int): List[ColumnConfiguration] = for {
    table <- tables.toList
    column <- table.allColumns
  } yield ColumnConfiguration(column, None)
  def getTables(version: Int): Set[TableConfiguration[_ <: DomainQuery, _ <: DomainQuery, _]] = tables
  def queryCanBeGrouped(query: DomainQuery): Option[ColumnConfiguration] = None
  def queryCanBeOrdered(query: DomainQuery): Option[ColumnConfiguration] = None
  def queryCanUseMaterializedView(query: DomainQuery, materializedView: MaterializedView): Option[(Boolean, Int)] = None
  def reInitialize(oldversion: Int): QueryspaceConfiguration = this
}

object TestQuerySpace {
    
  def createTableConfiguration[K](tableid: TableIdentifier, 
      primaryKeyColumns: List[Column], 
      clusteringKeyColumns: List[Column], 
      valueColumns: List[Column],
      data: Map[K, SimpleRow]): TableConfiguration[DomainQuery, DomainQuery, Row] = TableConfiguration[DomainQuery, DomainQuery, Row](
    tableid, None, 
    primaryKeyColumns.toSet,
    clusteringKeyColumns.toSet,
    valueColumns.toSet,
    row => row,
    query => query,
    None, //Some(() => createQueryableStore[K](data)),
    None, //Some(() => new MapStore(data)),
    List()
  )
  
//  def createQueryableStore[K](data: Map[K, SimpleRow]): QueryableSource = new QueryableSource {
//    override def queryable: ReadableSource[DomainQuery, Seq[Row]] = new ReadableSource[DomainQuery, Seq[Row]]{
//      override def get(k: DomainQuery): Future[Option[Seq[Row]]] = Future.value {
//        val result = for {
//          entry <- data.toSeq
//          column <- entry._2.columns
//          if k.domains.find{domain => domain.column == column.column && DomainFilterSource.domainCheck(column.value, domain, None)}.isEmpty
//        } yield entry._2
//        if(result.size > 0) {
//          Some(result)
//        } else None
//      }
//    }
//  } 
}