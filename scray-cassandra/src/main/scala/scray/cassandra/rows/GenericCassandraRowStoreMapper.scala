// See the LICENCE.txt file distributed with this work for additional
// information regarding copyright ownership.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.
package scray.cassandra.rows

import com.datastax.driver.core.{Row => CassRow, ColumnDefinitions}
import scray.querying.description.Row
import com.twitter.storehaus.cassandra.cql.CQLCassandraRowStore
import com.websudos.phantom.CassandraPrimitive
import scala.annotation.tailrec
import scala.collection.mutable.{ArrayBuffer, ListBuffer}
import scray.querying.description.{Column, RowColumn, SimpleRow, TableIdentifier}
import scray.cassandra.extractors.CassandraExtractor
import java.nio.ByteBuffer
import scala.collection.JavaConverters._

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
            case bytes: ByteBuffer =>
              val byteArray = new Array[Byte](bytes.remaining())
              bytes.get(byteArray)
              buf += RowColumn(Column(in.head.getName, ti), byteArray)
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
