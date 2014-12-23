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
import com.twitter.finagle.ListeningServer
import java.net.InetAddress
 
object ScrayStatefulTServer extends AbstractScrayStatefulTServer {
  override val endpoint = ScrayServerEndpoint(
      InetAddress.getByName(scray.core.service.ENDPOINT.split(":")(0)),
      Integer.valueOf(scray.core.service.ENDPOINT.split(":")(1))
  )
  
  override def initializeResources(): Unit = {}
  override def destroyResources(): Unit = {}
}

case class ScrayServerEndpoint(host: InetAddress, port: Int)

abstract class AbstractScrayStatefulTServer extends KryoPoolRegistration {
  val endpoint: ScrayServerEndpoint
  
  val VER = "1.7"
  
  lazy val server: ListeningServer = Thrift.serveIface(addressString, ScrayStatefulTServiceImpl)
 
  def addressString: String = s"${endpoint.host.getHostAddress}:${endpoint.port}"
  
  def initializeResources: Unit
  def destroyResources: Unit
  
  def main(args : Array[String]) {
    register
    initializeResources
    println(s"Server Version $VER")
    Await.ready(server)
  }
  
  def shutdown: Unit = {
    destroyResources
    server.close()
  }
}

trait KryoPoolRegistration {
  def register = RegisterRowCachingSerializers() 
}
