package scray.loader.tools

import scray.common.serialization.KryoPoolSerialization
import com.typesafe.scalalogging.slf4j.LazyLogging
import scray.common.serialization.BatchIDSerializer
import scray.common.serialization.BatchID
import scray.common.serialization.numbers.KryoSerializerNumber

/**
 * Tool object containing serialization helpers
 */
object SerializationTooling extends LazyLogging {

  /**
   * register additional Kryo serializers
   */
  def registerAdditionalKryoSerializers() = {
    logger.debug(s"Registering Kryo serializers for Scray")
    KryoPoolSerialization.register(classOf[BatchID], new BatchIDSerializer, KryoSerializerNumber.BatchId.getNumber)
  }


}