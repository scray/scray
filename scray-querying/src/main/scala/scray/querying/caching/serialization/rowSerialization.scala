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
package scray.querying.caching.serialization

import com.esotericsoftware.kryo.{ Kryo, Serializer => KSerializer }
import com.esotericsoftware.kryo.io.{ Input, Output }
import scray.querying.description.{ Column, CompositeRow, Row, RowColumn, TableIdentifier, SimpleRow }
import scala.collection.mutable.ArrayBuffer
import scala.annotation.tailrec
import scray.common.serialization.KryoPoolSerialization
import scray.common.serialization.numbers.KryoSerializerNumber

/**
 * convenience method to register these serializers
 */
object RegisterRowCachingSerializers {
  // it's safe to not lock this var because it's locked through cache-lock in the Registry 
  private var registered = false
  def apply() = {
    if(!registered) {
      registered = true
      KryoPoolSerialization.register(classOf[Column], new ColumnSerialization, KryoSerializerNumber.column.getNumber())
      KryoPoolSerialization.register(classOf[RowColumn[_]], new RowColumnSerialization, KryoSerializerNumber.rowcolumn.getNumber())
      KryoPoolSerialization.register(classOf[SimpleRow], new SimpleRowSerialization, KryoSerializerNumber.simplerow.getNumber())
      KryoPoolSerialization.register(classOf[CompositeRow], new CompositeRowSerialization, KryoSerializerNumber.compositerow.getNumber())
    }
  }
}

/**
 * fast serialization of CompositeRow
 */
class CompositeRowSerialization extends KSerializer[CompositeRow] {

  override def write(k: Kryo, o: Output, v: CompositeRow): Unit = {
    o.writeShort(v.rows.size)
    v.rows.foreach {
      case simple: SimpleRow => 
        o.writeByte(SIMPLE_ROW)
        k.writeObject(o, simple)
      case composite: CompositeRow => 
        o.writeByte(COMPOSITE_ROW)
        k.writeObject(o, composite)
    }
  }

  override def read(k: Kryo, i: Input, c: Class[CompositeRow]): CompositeRow = {
    val abuf = new ArrayBuffer[Row]
    @tailrec def deserializeRows(count: Int): Unit = {
      if(count > 0) {
        i.readByte() match {
          case SIMPLE_ROW => abuf += k.readObject(i, classOf[SimpleRow])
          case COMPOSITE_ROW => abuf += k.readObject(i, classOf[CompositeRow])
        }
        deserializeRows(count - 1)
      }
    }
    val number = i.readShort
    deserializeRows(number)
    new CompositeRow(abuf.toList)
  }
}

/**
 * fast serialization of SimpleRow
 */
class SimpleRowSerialization extends KSerializer[SimpleRow] {

  override def write(k: Kryo, o: Output, v: SimpleRow): Unit = {
    o.writeShort(v.columns.size)
    v.columns.foreach(k.writeObject(o, _))
  }
  
  override def read(k: Kryo, i: Input, c: Class[SimpleRow]): SimpleRow = {
    val abuf = new ArrayBuffer[RowColumn[_]]
    @tailrec def deserializeColumns(count: Int): Unit = {
      if(count > 0) {
        abuf += k.readObject(i, classOf[RowColumn[_]])
        deserializeColumns(count - 1)
      }
    }
    val number = i.readShort
    deserializeColumns(number)
    SimpleRow(abuf)
  }
}

/**
 * fast serialization of RowColumn
 */
class RowColumnSerialization extends KSerializer[RowColumn[_]] {

  override def write(k: Kryo, o: Output, v: RowColumn[_]): Unit = {
    k.writeObject(o, v.column)
    k.writeClassAndObject(o, v.value)
  }

  override def read(k: Kryo, i: Input, c: Class[RowColumn[_]]): RowColumn[_] = {
    val column = k.readObject(i, classOf[Column])
    RowColumn(column, k.readClassAndObject(i))
  }
}

/**
 * fast serialization for Columns
 */
class ColumnSerialization extends KSerializer[Column] {

  override def write(k: Kryo, o: Output, v: Column): Unit = {
    o.writeString(v.table.dbSystem)
    o.writeString(v.table.dbId)
    o.writeString(v.table.tableId)
    o.writeString(v.columnName)
  }

  override def read(k: Kryo, i: Input, c: Class[Column]): Column = {
    val dbSystem = i.readString
    val dbId = i.readString
    val tableId = i.readString
    val table = TableIdentifier(dbSystem, dbId, tableId)
    Column(i.readString, table)
  }
}

