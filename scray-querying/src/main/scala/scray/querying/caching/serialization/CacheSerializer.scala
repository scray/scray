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

/**
 * serializer usable with key-value-caches
 */
class KeyValueCacheSerializer extends Serializer[Row] {

  override def serialize(out: DataOutput, value: Row) = {
    val typ = value match {
      case simple: SimpleRow => SIMPLE_ROW
      case composite: CompositeRow => COMPOSITE_ROW
    }
    val bytes = KryoPoolSerialization.chill.toBytesWithoutClass(value)
    out.writeInt(bytes.length)
    out.write(bytes)
    out.writeByte(typ)
  }

  override def deserialize(in: DataInput, available: Int): Row = {
    val bytes = new Array[Byte](in.readInt)
    in.readFully(bytes)
    in.readByte match {
      case SIMPLE_ROW => KryoPoolSerialization.chill.fromBytes(bytes, classOf[SimpleRow])
      case COMPOSITE_ROW => KryoPoolSerialization.chill.fromBytes(bytes, classOf[CompositeRow])
    }
  }

  override def fixedSize: Int = -1
}
