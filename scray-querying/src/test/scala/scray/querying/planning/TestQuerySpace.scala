//package scray.querying.planning
//
//import scray.querying.description.QueryspaceConfiguration
//import scray.querying.description.internal.MaterializedView
//import scray.querying.queries.DomainQuery
//import scray.querying.description.ColumnConfiguration
//import scray.querying.description.TableConfiguration
//import scray.querying.description.Column
//import scray.querying.description.ColumnConfiguration
//import scray.querying.description.TableConfiguration
//import scray.querying.description.TableIdentifier
//import com.twitter.storehaus.MapStore
//import com.twitter.storehaus.QueryableStore
//import com.twitter.storehaus.ReadableStore
//import scray.querying.description.Row
//import com.twitter.util.Future
//import scray.querying.description.SimpleRow
//import scray.querying.source.DomainFilterSource
//
//class TestQuerySpace(tables: Set[TableConfiguration[_, _, _]]) extends QueryspaceConfiguration("test") {
//  
//  def getColumns(version: Int): List[ColumnConfiguration] = for {
//    table <- tables.toList
//    column <- table.allColumns
//  } yield ColumnConfiguration(column, this, None)
//  def getTables(version: Int): Set[TableConfiguration[_, _, _]] = tables
//  def queryCanBeGrouped(query: DomainQuery): Option[ColumnConfiguration] = None
//  def queryCanBeOrdered(query: DomainQuery): Option[ColumnConfiguration] = None
//  def queryCanUseMaterializedView(query: DomainQuery, materializedView: MaterializedView): Option[(Boolean, Int)] = None
//  def reInitialize(oldversion: Int): QueryspaceConfiguration = this
//}
//
//object TestQuerySpace {
//  
//  def createTableConfiguration[K](name: String, primaryKeyColumns: List[Column], clusteringKeyColumns: List[Column], 
//      valueColumns: List[Column], data: Map[K, SimpleRow]): TableConfiguration[DomainQuery, K, Row] = 
//    createTableConfiguration[K](TableIdentifier("test", "test", name), primaryKeyColumns, clusteringKeyColumns, valueColumns, data)
//  
//  def createTableConfiguration[K](tableid: TableIdentifier, 
//      primaryKeyColumns: List[Column], 
//      clusteringKeyColumns: List[Column], 
//      valueColumns: List[Column],
//      data: Map[K, SimpleRow]): TableConfiguration[DomainQuery, K, Row] = TableConfiguration[DomainQuery, K, Row](
//    tableid, None, 
//    primaryKeyColumns.toSet,
//    clusteringKeyColumns.toSet,
//    valueColumns.toSet,
//    row => row,
//    query => query,
//    Some(() => createQueryableStore[K](data)),
//    Some(() => new MapStore(data)),
//    List()
//  )
//  
//  def createQueryableStore[K](data: Map[K, SimpleRow]): QueryableStore[DomainQuery, Row] = new QueryableStore[DomainQuery, Row] {
//    override def queryable: ReadableStore[DomainQuery, Seq[Row]] = new ReadableStore[DomainQuery, Seq[Row]]{
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
//  
//}