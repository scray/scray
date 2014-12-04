package scray.common.serialization

import com.twitter.chill.{KryoPool, ScalaKryoInstantiator}
import com.twitter.chill.AllScalaRegistrar
import com.esotericsoftware.kryo.Serializer
import scala.collection.mutable.ArrayBuffer

/**
 * Generic static thread-safe pool to serialize and de-serialize stuff using kryo
 */
object KryoPoolSerialization {

  case class SerializerEntry[T](val cls: Class[T], ser: Serializer[T])
  
  val POOL_SIZE = 10;
  
  private val instantiator = new ScrayKryoInstantiator
  private val serializers = new ArrayBuffer[SerializerEntry[_]]
  
  def getSerializers = serializers.toList
  
  lazy val chill = KryoPool.withByteArrayOutputStream(POOL_SIZE, instantiator)
  
  def register[T](cls: Class[T], serializer: Serializer[T]): Unit = {
    serializers += SerializerEntry(cls, serializer)
  }
}

/**
 * a registrar that allows to add our own serializers
 */
class ScrayKryoInstantiator extends ScalaKryoInstantiator {
  override def newKryo = {
    val k = super.newKryo
    k.setRegistrationRequired(true)
    val reg = new AllScalaRegistrar
    reg(k)
    KryoPoolSerialization.getSerializers.foreach(ser => k.register(ser.cls, ser.ser))
    k
  }
}