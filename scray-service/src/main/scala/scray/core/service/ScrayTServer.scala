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
 
package scray.core.service
 
import com.esotericsoftware.kryo.Kryo
import com.twitter.finagle.Thrift
import com.twitter.util.Await
import scray.querying.description._
import scray.querying.caching.serialization._
import scray.common.serialization.KryoPoolSerialization
import scray.common.serialization.KryoSerializerNumber
 
object ScrayTServer extends KryoPoolRegistration {
 
  val server = Thrift.serveIface(scray.core.service.ENDPOINT, ScrayTServiceImpl)
 
  def main(args : Array[String]) {
    register
    Await.ready(server)
  }
}
 
trait KryoPoolRegistration {
  def register = {
    KryoPoolSerialization.register(classOf[Column], new ColumnSerialization, KryoSerializerNumber.column.getNumber())
    KryoPoolSerialization.register(classOf[RowColumn[_]], new RowColumnSerialization, KryoSerializerNumber.rowcolumn.getNumber())
    KryoPoolSerialization.register(classOf[SimpleRow], new SimpleRowSerialization, KryoSerializerNumber.simplerow.getNumber())
    KryoPoolSerialization.register(classOf[CompositeRow], new CompositeRowSerialization, KryoSerializerNumber.compositerow.getNumber())
  }
}