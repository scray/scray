package scray.cassandra.rows

import com.twitter.storehaus.cassandra.cql.CQLCassandraCollectionStore
import scray.querying.description.SimpleRow
import scray.querying.description.TableIdentifier
import scala.collection.mutable.ArrayBuffer
import scray.querying.description.Row
import scray.querying.description.RowColumn
import scray.cassandra.extractors.CassandraExtractor
import scray.querying.description.Column
import shapeless._
import com.websudos.phantom.CassandraPrimitive
import com.twitter.storehaus.cassandra.cql.CassandraTupleStore
import com.twitter.summingbird.batch.BatchID


object CassandraGenericIndexRowMappers {

  // Cassandra types for time indexes
  type TimeIndexRK = Int :: HNil
  type TimeIndexCK = Long :: BatchID :: HNil
  type TimeIndexValueType = Set[String]
  type TimeIndexRS = CassandraPrimitive[Int] :: HNil
  type TimeIndexCS = CassandraPrimitive[Long] :: CassandraPrimitive[BatchID] :: HNil
  type TimeIndexStore = CQLCassandraCollectionStore[TimeIndexRK, TimeIndexCK,
      TimeIndexValueType, String, TimeIndexRS, TimeIndexCS]
  
  def timeIndexRowMapper(store: TimeIndexStore, tableName: Option[String]): 
        (((TimeIndexRK, TimeIndexCK), TimeIndexValueType)) => Row = (entry) => {
    val ti = tableName.map(TableIdentifier(CassandraExtractor.DB_ID, store.columnFamily.session.getKeyspacename, _)).getOrElse {
      TableIdentifier(CassandraExtractor.DB_ID, store.columnFamily.session.getKeyspacename, store.columnFamily.getName)}
    val rows = new ArrayBuffer[RowColumn[_]]()
    // scalastyle:off magic.number
    rows += RowColumn(Column(store.rowkeyColumnNames(0), ti), entry._1._1.head)
    rows += RowColumn(Column(store.colkeyColumnNames(0), ti), entry._1._2.head)
    rows += RowColumn(Column(store.colkeyColumnNames(1), ti), entry._1._2.tail.head)
    rows += RowColumn(Column(store.valueColumnName, ti), entry._2)
    // scalastyle:on magic.number
    SimpleRow(rows)
  }
        
  // Cassandra types for a*/*a indexes
}