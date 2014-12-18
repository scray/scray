package scray.cassandra.rows

import com.datastax.driver.core.{Row => CassRow}
import scray.querying.description.Row
import com.twitter.storehaus.cassandra.cql.CQLCassandraRowStore
import com.websudos.phantom.CassandraPrimitive
import scala.annotation.tailrec
import scala.collection.mutable.ArrayBuffer
import scray.querying.description.RowColumn
import com.datastax.driver.core.ColumnDefinitions
import scray.querying.description.TableIdentifier
import scray.querying.description.Column
import scray.cassandra.extractors.CassandraExtractor
import scala.collection.mutable.ListBuffer
import scray.querying.description.SimpleRow
import java.nio.ByteBuffer

/**
 * a generic mapper to map cassandra rows emitted by CQLCassandraRowStore to scray rows 
 */
object GenericCassandraRowStoreMapper {

  val defaultCassPrimitives: List[CassandraPrimitive[_]] = List(
    CassandraPrimitive.BigDecimalCassandraPrimitive,
    CassandraPrimitive.BigIntCassandraPrimitive,
    CassandraPrimitive.BlobIsCassandraPrimitive,
    CassandraPrimitive.BooleanIsCassandraPrimitive,
    CassandraPrimitive.DateIsCassandraPrimitive,
    CassandraPrimitive.DoubleIsCassandraPrimitive,
    CassandraPrimitive.FloatIsCassandraPrimitive,
    CassandraPrimitive.InetAddressCassandraPrimitive,
    CassandraPrimitive.IntIsCassandraPrimitive,
    CassandraPrimitive.LongIsCassandraPrimitive,
    CassandraPrimitive.StringIsCassandraPrimitive,
    CassandraPrimitive.UUIDIsCassandraPrimitive
  )
  
  val additionalCassMappings: List[(String, CassandraPrimitive[_])] = List(
    "varchar" -> CassandraPrimitive.StringIsCassandraPrimitive,
    "ascii" -> CassandraPrimitive.StringIsCassandraPrimitive,
    "counter" -> CassandraPrimitive.LongIsCassandraPrimitive,
    "timeuuid" -> CassandraPrimitive.UUIDIsCassandraPrimitive
  )
  
  def cassTypeMap(cassPrimitives: List[CassandraPrimitive[_]], additionalMappings: List[(String, CassandraPrimitive[_])] = List()): 
    Map[String, CassandraPrimitive[_]] = (cassPrimitives.map(prim => prim.cassandraType -> prim) ++ additionalMappings).toMap
  
  implicit val cassandraPrimitiveTypeMap = cassTypeMap(defaultCassPrimitives, additionalCassMappings)
    
  def rowMapper[K: CassandraPrimitive](store: CQLCassandraRowStore[K], tableName: Option[String])(implicit typeMap: Map[String, CassandraPrimitive[_]]): 
    ((K, CassRow)) => Row = (kv) => {
    import scala.collection.JavaConverters._
    val (key, cassrow) = kv
    val ti = tableName.map(TableIdentifier(CassandraExtractor.DB_ID, store.columnFamily.session.getKeyspacename, _)).getOrElse {
        TableIdentifier(CassandraExtractor.DB_ID, store.columnFamily.session.getKeyspacename, store.columnFamily.getName)}
    @tailrec def columnsTransform(in: List[ColumnDefinitions.Definition], buf: ListBuffer[RowColumn[_]]): ListBuffer[RowColumn[_]] = 
      if(in.isEmpty) {
        buf
      } else {
        val tm = typeMap.get(in.head.getType.getName.toString).get.fromRow(cassrow, in.head.getName)
        val buffer = tm match {
          case Some(value) => value match {
            case bytes: ByteBuffer => buf += RowColumn(Column(in.head.getName, ti), bytes.array())
            case _ => buf += RowColumn(Column(in.head.getName, ti), value)
          }
          case None => buf
        }
        columnsTransform(in.tail, buffer)
      }
    val lb = columnsTransform(cassrow.getColumnDefinitions().asList().asScala.toList, ListBuffer.empty[RowColumn[_]])
    SimpleRow(new ArrayBuffer[RowColumn[_]](lb.size).++=(lb))
  }
}