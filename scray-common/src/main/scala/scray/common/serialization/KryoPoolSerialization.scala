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
package scray.common.serialization

import com.twitter.chill.{KryoPool, ScalaKryoInstantiator}
import com.twitter.chill.AllScalaRegistrar
import com.esotericsoftware.kryo.Serializer
import scala.collection.mutable.ArrayBuffer

/**
 * Generic static thread-safe pool to serialize and de-serialize stuff using kryo
 */
object KryoPoolSerialization {

  case class SerializerEntry[T](val cls: Class[T], ser: Serializer[T], num: Int)
  
  val POOL_SIZE = 10;
  
  private val instantiator = new ScrayKryoInstantiator
  private val serializers = new ArrayBuffer[SerializerEntry[_]]
  
  def getSerializers = serializers.toList
  
  lazy val chill = KryoPool.withByteArrayOutputStream(POOL_SIZE, instantiator)
  
  def register[T](cls: Class[T], serializer: Serializer[T], number: Int): Unit = {
    serializers += SerializerEntry(cls, serializer, number)
  }
}

/**
 * a registrar that allows to add our own serializers
 */
class ScrayKryoInstantiator extends ScalaKryoInstantiator {
  override def newKryo = {
    val k = super.newKryo
    k.setRegistrationRequired(false)
    val reg = new AllScalaRegistrar
    reg(k)
    KryoPoolSerialization.getSerializers.foreach(ser => k.register(ser.cls, ser.ser, ser.num))
    k
  }
}