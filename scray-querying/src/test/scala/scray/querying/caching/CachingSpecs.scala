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
import scray.querying.caching.serialization.{ ColumnSerialization, CompositeRowSerialization, RegisterRowCachingSerializers, RowColumnSerialization, SimpleRowSerialization }
import scray.querying.description.{ Column, CompositeRow, RowColumn, SimpleRow, TableIdentifier }
import java.io.File
import java.util.UUID
import scray.common.serialization.{ KryoPoolSerialization, JavaKryoRowSerialization, JavaSimpleRow, JavaColumn, JavaCompositeRow, JavaRowColumn }
import com.twitter.chill.java.UUIDSerializer
import org.mapdb.DBMaker
import scray.querying.caching.serialization.KeyValueCacheSerializer
import scray.querying.description.Row
import java.nio.ByteBuffer
import sun.security.provider.MD5
import scala.util.hashing.MurmurHash3
import com.esotericsoftware.minlog.Log


/**
 * Scray querying specification.
 */
@RunWith(classOf[JUnitRunner])
class CachingSpecs extends WordSpec {
  val bla = "bla"
  val ti = TableIdentifier("cassandra", "mytestspace", "mycf")
  val t2 = TableIdentifier("cassandra", "mytestsdfspace", "mysdfcf")
  val si = SimpleRow(ArrayBuffer(RowColumn(Column(bla + "1", ti), 1), RowColumn(Column(bla + "1", ti), "blubb")))
  val s2 = SimpleRow(ArrayBuffer(RowColumn(Column(bla + "sd1", ti), 1), RowColumn(Column(bla + "sd2", t2), UUID.randomUUID()),
      RowColumn(Column(bla + "sd3", t2), 2.3d)))
  "Scray's caches" should {
    "(de)serialize SimpleRows using kryo" in {
      // useful to look at the output afterwards
      val filename = System.getProperty("java.io.tmpdir") + System.getProperty("file.separator") + "scraytest.txt"
      checkAndDeleteIfExisting(filename)
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
    "create smaller kryo hand-made (de)serialize for SimpleRows using fieldserializer alone" in {
      // compare serilaization sizes - make sure we use the smallest possible footprint
      val filename1 = System.getProperty("java.io.tmpdir") + System.getProperty("file.separator") + "scraytest1.txt"
      val filename2 = System.getProperty("java.io.tmpdir") + System.getProperty("file.separator") + "scraytest2.txt"
      checkAndDeleteIfExisting(filename1)
      checkAndDeleteIfExisting(filename2)
      val k = new Kryo
      val str = new FileOutputStream(filename1)
      val output = new Output(str)
      k.writeObject(output, new CompositeRow(List(si, si)))
      output.close()
      str.close
      k.register(classOf[Column], new ColumnSerialization)
      k.register(classOf[RowColumn[_]], new RowColumnSerialization)
      k.register(classOf[SimpleRow], new SimpleRowSerialization)
      k.register(classOf[CompositeRow], new CompositeRowSerialization)
      val str2 = new FileOutputStream(filename2)
      val output2 = new Output(str2)
      k.writeObject(output2, new CompositeRow(List(si, si)))
      output2.close()
      str2.close
      val size1 = new File(filename1).length
      val size2 = new File(filename2).length
      assert(size2 <= size1)
    }
    "interop with specialized java de-serialization for Simple- and CompositeRows" in {
      val filename1 = System.getProperty("java.io.tmpdir") + System.getProperty("file.separator") + "scraytest3.txt"
      val filename2 = System.getProperty("java.io.tmpdir") + System.getProperty("file.separator") + "scraytest4.txt"
      checkAndDeleteIfExisting(filename1)
      checkAndDeleteIfExisting(filename2)
      val k = new Kryo
      k.register(classOf[UUID], new UUIDSerializer)
      k.register(classOf[Column], new ColumnSerialization)
      k.register(classOf[RowColumn[_]], new RowColumnSerialization)
      k.register(classOf[SimpleRow], new SimpleRowSerialization)
      k.register(classOf[CompositeRow], new CompositeRowSerialization)
      val str = new FileOutputStream(filename1)
      val output = new Output(str)
      k.writeObject(output, si)
      output.close()
      str.close
      val str2 = new FileOutputStream(filename2)
      val output2 = new Output(str2)
      k.writeObject(output2, new CompositeRow(List(si, s2)))
      output2.close()
      str2.close
      // now read that stuff out from the files using a different registration
      val k2 = new Kryo
      k2.register(classOf[UUID], new UUIDSerializer)
      JavaKryoRowSerialization.registerSerializers(k2)
      val istr = new Input(new FileInputStream(filename1))
      val result = k2.readObject(istr, classOf[JavaSimpleRow])
      assert(result.isInstanceOf[JavaSimpleRow])
	  assert(result.getColumns().size() == 2)
	  assert(result.getColumns().get(0).getValue() == 1)
	  assert(result.getColumns().get(1).getValue() == "blubb")
      val istr2 = new Input(new FileInputStream(filename2))
      val result2 = k2.readObject(istr2, classOf[JavaCompositeRow])
      assert(result2.isInstanceOf[JavaCompositeRow])
	  assert(result2.getRows().size() == 2)
	  val row1 = result2.getRows().get(0).asInstanceOf[JavaSimpleRow]
	  val row2 = result2.getRows().get(1).asInstanceOf[JavaSimpleRow]
	  assert(row1.getColumns().size() == 2)
	  assert(row2.getColumns().size() == 3)
	  assert(row1.getColumns().get(0).getValue() == 1)
	  assert(row2.getColumns().get(1).getValue().isInstanceOf[UUID])
	  assert(row2.getColumns().get(2).getValue() == 2.3d)
    }
    "allow off-heap cache storage for Simple- and CompositeRows" in {
      val db = DBMaker.newMemoryDirectDB().transactionDisable().asyncWriteEnable().make
      val cache = db.createHashMap("cache").counterEnable().valueSerializer(new KeyValueCacheSerializer()).make[UUID, Row]
      try {
        val randUUID = UUID.randomUUID()
        val randUUID2 = UUID.randomUUID()
        val comRow = new CompositeRow(List(si, s2))
        cache.put(randUUID2, comRow)
        cache.put(randUUID, s2)
        assert(cache.get(randUUID) == s2)
        assert(comRow eq cache.get(randUUID2))
      } finally {
        cache.close
      }
    }
  }
  
  private def checkAndDeleteIfExisting(pathName: String): Unit = {
    val f = new File(pathName)
    if(f.exists() && !f.isDirectory()) {
      f.delete()
    }
  }
}
