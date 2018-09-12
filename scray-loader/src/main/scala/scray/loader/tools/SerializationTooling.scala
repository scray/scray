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
package scray.loader.tools

import scray.common.serialization.KryoPoolSerialization
import com.typesafe.scalalogging.LazyLogging
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
  def registerAdditionalKryoSerializers(): Unit = {
    logger.debug(s"Registering Kryo serializers for Scray")
    KryoPoolSerialization.register(classOf[BatchID], new BatchIDSerializer, KryoSerializerNumber.BatchId.getNumber)
  }
}
