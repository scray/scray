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
package scray.querying.caching

import com.esotericsoftware.kryo.Kryo
import com.esotericsoftware.kryo.io.{ Input, Output }
import java.io.{ ByteArrayInputStream, FileInputStream, FileOutputStream }
import org.junit.runner.RunWith
import org.scalatest.WordSpec
import org.scalatest.junit.JUnitRunner
import scala.collection.mutable.ArrayBuffer
import scray.querying.caching.serialization.{ ColumnSerialization, RegisterRowCachingSerializers, RowColumnSerialization, SimpleRowSerialization }
import scray.querying.description.{ Column, CompositeRow, RowColumn, SimpleRow, TableIdentifier }  
import scray.common.serialization.KryoPoolSerialization


/**
 * Scray querying specification.
 */
@RunWith(classOf[JUnitRunner])
class CachingSpecs extends WordSpec {
  val bla = "bla"
  val ti = TableIdentifier("cassandra", "mytestspace", "mycf")
  val si = SimpleRow(ArrayBuffer(RowColumn(Column(bla + "1", ti), 1), RowColumn(Column(bla + "1", ti), "blubb")))
  "Scray's caches" should {
    "(de)serialize SimpleRows using kryo" in {
      // useful to look at the output afterwards
      val filename = System.getProperty("java.io.tmpdir") + System.getProperty("file.separator") + "scraytest.txt"
      val k = new Kryo
      k.register(classOf[Column], new ColumnSerialization)
      k.register(classOf[RowColumn[_]], new RowColumnSerialization)
      k.register(classOf[SimpleRow], new SimpleRowSerialization)
      val str = new FileOutputStream(filename)
      val output = new Output(str)
      k.writeObject(output, si)
      output.close()
      str.close
      val istr = new Input(new FileInputStream(filename))
      val result = k.readObject(istr, classOf[SimpleRow])
      assert(si == result)
    }
    "(de)serialize SimpleRows using KryoPool" in {
      RegisterRowCachingSerializers()
      val bytes = KryoPoolSerialization.chill.toBytesWithoutClass(si)
      val result = KryoPoolSerialization.chill.fromBytes(bytes, classOf[SimpleRow])
      assert(si == result)
    }
    "(de)serialize CompositeRows using KryoPool" in {
      RegisterRowCachingSerializers()
      val bytes = KryoPoolSerialization.chill.toBytesWithoutClass(new CompositeRow(List(si, si)))
      val result = KryoPoolSerialization.chill.fromBytes(bytes, classOf[CompositeRow])
      assert(si == result.rows(1))
    }
  }
}
