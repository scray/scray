package scray.core.service.spools.memcached

import scala.util.Try
import org.jboss.netty.buffer.ChannelBuffer
import com.twitter.bijection.Injection
import com.twitter.bijection.netty.ChannelBufferBijection
import com.twitter.finagle.memcachedx.Client
import com.twitter.storehaus.ConvertedStore
import com.twitter.storehaus.memcache.MemcacheStore
import com.twitter.util.Duration
import MemcachePageStore.pageInjection
import scray.core.service.spools.PageValue

object MemcachePageStore {
  private implicit val cb2ary = ChannelBufferBijection

  // an injection serializing pages via chill (requires respective chill serializer)
  private[memcached] implicit val page2bytes = Injection.build[PageValue, Array[Byte]](
    pooledKryoInjection(_))(
      pooledKryoInjection.invert(_) flatMap (obj => Try(obj.asInstanceOf[PageValue])))

  // an injection connecting chill serialization with channel buffer conversion
  private[memcached] implicit val pageInjection : Injection[PageValue, ChannelBuffer] =
    Injection.connect[PageValue, Array[Byte], ChannelBuffer]

  def apply(client : Client, ttl : Duration = MemcacheStore.DEFAULT_TTL, flag : Int = MemcacheStore.DEFAULT_FLAG) =
    new MemcachePageStore(MemcacheStore(client, ttl, flag))
}

class MemcachePageStore(underlying : MemcacheStore)
  extends ConvertedStore[String, String, ChannelBuffer, PageValue](underlying)(identity)
