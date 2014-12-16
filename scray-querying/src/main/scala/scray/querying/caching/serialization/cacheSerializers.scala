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

import org.mapdb.Serializer
import scray.querying.description.Row
import java.io.DataOutput
import java.io.DataInput
import scray.querying.description.SimpleRow
import scray.querying.description.CompositeRow
import scray.common.serialization.KryoPoolSerialization
import scray.querying.caching.IncompleteCacheServeMarkerRow
import scray.querying.caching.CompleteCacheServeMarkerRow
import scala.annotation.tailrec
import scala.collection.mutable.ArrayBuffer

/**
 * serializer usable with key-value-caches
 */
class KeyValueCacheSerializer extends Serializer[Row] with Serializable {

  override def serialize(out: DataOutput, value: Row) = {
    val typ = value match {
      case simple: SimpleRow => SIMPLE_ROW
      case composite: CompositeRow => COMPOSITE_ROW
      case incomplete: IncompleteCacheServeMarkerRow => CACHE_INCOMPLETE_MARKER_ROW
      case complete: CompleteCacheServeMarkerRow => CACHE_COMPLETE_MARKER_ROW
    }
    out.writeByte(typ)
    if(typ != CACHE_INCOMPLETE_MARKER_ROW && typ != CACHE_COMPLETE_MARKER_ROW) {
      val bytes = KryoPoolSerialization.chill.toBytesWithoutClass(value)
      out.writeInt(bytes.length)
      out.write(bytes)
    }
  }

  override def deserialize(in: DataInput, available: Int): Row = {
    in.readByte match {
      case SIMPLE_ROW => 
        val bytes = new Array[Byte](in.readInt)
        in.readFully(bytes)
        KryoPoolSerialization.chill.fromBytes(bytes, classOf[SimpleRow])
      case COMPOSITE_ROW =>
        val bytes = new Array[Byte](in.readInt)
        in.readFully(bytes)
        KryoPoolSerialization.chill.fromBytes(bytes, classOf[CompositeRow])
      case CACHE_INCOMPLETE_MARKER_ROW => new IncompleteCacheServeMarkerRow
      case CACHE_COMPLETE_MARKER_ROW => new CompleteCacheServeMarkerRow
    }
  }

  override def fixedSize: Int = -1
}

/**
 * serializer usable with index caches
 */
class QueryableCacheSerializer extends Serializer[ArrayBuffer[Row]] with Serializable {

  override def serialize(out: DataOutput, value: ArrayBuffer[Row]) = {
    val size = value.size
    out.writeInt(size)
    @tailrec def serializeRows(index: Int): Unit = if(index < size) {
      val typ = value(index) match {
        case simple: SimpleRow => SIMPLE_ROW
        case composite: CompositeRow => COMPOSITE_ROW
        case incomplete: IncompleteCacheServeMarkerRow => CACHE_INCOMPLETE_MARKER_ROW
        case complete: CompleteCacheServeMarkerRow => CACHE_COMPLETE_MARKER_ROW
      }
      out.writeByte(typ)
      if(typ != CACHE_INCOMPLETE_MARKER_ROW && typ != CACHE_COMPLETE_MARKER_ROW) {
        val bytes = KryoPoolSerialization.chill.toBytesWithoutClass(value(index))
        out.writeInt(bytes.length)
        out.write(bytes)
      }
      serializeRows(index + 1)
    }
    serializeRows(0)  
  }

  override def deserialize(in: DataInput, available: Int): ArrayBuffer[Row] = {
    val size = in.readInt
    val result = new ArrayBuffer[Row](size)
    @tailrec def deserializeRows(index: Int): Unit = if(index < size) {
      in.readByte match {
        case SIMPLE_ROW => 
          val bytes = new Array[Byte](in.readInt)
          in.readFully(bytes)
          KryoPoolSerialization.chill.fromBytes(bytes, classOf[SimpleRow])
        case COMPOSITE_ROW =>
          val bytes = new Array[Byte](in.readInt)
          in.readFully(bytes)
          KryoPoolSerialization.chill.fromBytes(bytes, classOf[CompositeRow])
        case CACHE_INCOMPLETE_MARKER_ROW => new IncompleteCacheServeMarkerRow
        case CACHE_COMPLETE_MARKER_ROW => new CompleteCacheServeMarkerRow
      }
      deserializeRows(index + 1)
    }
    deserializeRows(0)
    result
  }

  override def fixedSize: Int = -1
}


