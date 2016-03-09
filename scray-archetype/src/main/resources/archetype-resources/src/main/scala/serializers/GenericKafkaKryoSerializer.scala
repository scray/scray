package ${package}.serializers

import com.esotericsoftware.kryo.io.Input
import kafka.serializer.Decoder
import kafka.serializer.Encoder
import java.io.ByteArrayInputStream
import com.esotericsoftware.kryo.Kryo
import com.esotericsoftware.kryo.io.Output
import java.io.ByteArrayOutputStream
import com.twitter.chill.AllScalaRegistrar

/**
 * A generic kryo serializer to be used with Kafka
 * It is somewhat expensive in the sense that it always writes the class information.
 * However, it is generic and can easily be used without writing special serializers.
 * @author: Andreas Petter <andreas.petter@gmail.com>
 */
class GenericKafkaKryoSerializer[T <: Any](registerScalaCollections: Boolean = true) extends Decoder[T] with Encoder[T] {

  val kryo = new Kryo()
  if(registerScalaCollections) {
    new AllScalaRegistrar()(kryo)
  }

  override def fromBytes(bytes: Array[Byte]): T = {
    val input = new Input(new ByteArrayInputStream(bytes))
    kryo.readClassAndObject(input).asInstanceOf[T]
  }

  override def toBytes(that: T): Array[Byte] = {
    val output = new Output(new ByteArrayOutputStream())
    kryo.writeClassAndObject(output, that)
    val result = output.getBuffer()
    output.close()
    result
  }
}
