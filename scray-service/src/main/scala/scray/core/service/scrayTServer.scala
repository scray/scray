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
import scray.common.serialization.numbers.KryoSerializerNumber
import com.twitter.finagle.ListeningServer
import java.net.InetAddress
import scray.common.properties.ScrayProperties
import com.twitter.util.Try
import scray.common.properties.IntProperty
import scray.common.properties.predefined.PredefinedProperties

case class ScrayServerEndpoint(host : InetAddress, port : Int)

trait KryoPoolRegistration {
  def register = RegisterRowCachingSerializers()
  def registerProperties = {
    Try(ScrayProperties.registerProperty(PredefinedProperties.RESULT_COMPRESSION_MIN_SIZE))
  }
}

abstract class ScrayStatefulTServer extends AbstractScrayTServer {
  val server = Thrift.serveIface(addressString, ScrayStatefulTServiceImpl())
  override def getServer : ListeningServer = server
  override def getVersion : String = "1.7"
}

abstract class ScrayStatelessTServer extends AbstractScrayTServer {
  val server = Thrift.serveIface(addressString, ScrayStatelessTServiceImpl())
  override def getServer : ListeningServer = server
  override def getVersion : String = "0.9"
}

object ScrayStatelessTServerTest extends ScrayStatelessTServer {
  def initializeResources : Unit = {}
  def destroyResources : Unit = {}
  override def main(args : Array[String]) = super.main(args)
}

abstract class AbstractScrayTServer extends KryoPoolRegistration {

  def initializeResources : Unit
  def destroyResources : Unit
  def getServer : ListeningServer
  def getVersion : String

  val endpoint : ScrayServerEndpoint = ScrayServerEndpoint(
    InetAddress.getByName(scray.core.service.ENDPOINT.split(":")(0)),
    Integer.valueOf(scray.core.service.ENDPOINT.split(":")(1)))

  def addressString : String = s"${endpoint.host.getHostAddress}:${endpoint.port}"

  def main(args : Array[String]) {
    register // kryo pool registrars
    initializeResources
    println(s"Server Version ${getVersion}")
    Await.ready(getServer)
  }

  def shutdown : Unit = {
    destroyResources
    getServer.close()
  }
}
