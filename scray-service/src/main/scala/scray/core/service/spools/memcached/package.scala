package scray.core.service.spools

import java.util.UUID
import scala.util.Try
import com.twitter.bijection._
import com.twitter.chill.KryoInjection
import scray.common.serialization.KryoPoolSerialization
import com.twitter.storehaus.memcache.HashEncoder
import com.twitter.util.Duration

package object memcached {

  // time-to-life of memcached query results 
  final val DEFAULT_TTL : Duration = Duration.fromSeconds(180)

  val pooledKryoInjection = KryoInjection.instance(KryoPoolSerialization.chill)

  implicit val pageKeyCodec : Codec[PageKey] = Injection.build[PageKey, Array[Byte]](
    pooledKryoInjection(_))(
      pooledKryoInjection.invert(_) flatMap (obj => Try(obj.asInstanceOf[PageKey])))

  // unique namespace of scray memcached keys
  final val NAMESPACE = "scray"

  // memcached page key generator
  val pidKeyEncoder : PageKey => String = HashEncoder.keyEncoder[PageKey](namespace = NAMESPACE)

}
