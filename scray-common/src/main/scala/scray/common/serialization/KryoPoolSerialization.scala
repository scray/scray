package scray.querying.caching.serialization

import com.twitter.chill.{KryoPool, ScalaKryoInstantiator}

object KryoPoolSerialization {

  val POOL_SIZE = 10;
  
  val chill = KryoPool.withByteArrayOutputStream(POOL_SIZE, new ScalaKryoInstantiator)  
}