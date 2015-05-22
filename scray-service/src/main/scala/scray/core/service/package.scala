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

package scray.core

import java.util.UUID
import scray.service.qmodel.thrifscala.ScrayUUID
import java.net.InetAddress
import scray.core.service.properties.ScrayServicePropertiesRegistration
import scray.common.properties.ScrayProperties
import scray.common.properties.predefined.PredefinedProperties
import java.net.InetSocketAddress
import scray.core.service.properties.ScrayServicePropertiesRegistrar

package object service {

  // scray query endpoint
  val SCRAY_QUERY_ENDPOINT = new InetSocketAddress(
    ScrayProperties.getPropertyValue(PredefinedProperties.SCRAY_SERVICE_HOST),
    ScrayProperties.getPropertyValue(PredefinedProperties.SCRAY_QUERY_PORT))

  // scray meta endpoint
  val SCRAY_META_ENDPOINT = new InetSocketAddress(
    ScrayProperties.getPropertyValue(PredefinedProperties.SCRAY_SERVICE_HOST),
    ScrayProperties.getPropertyValue(PredefinedProperties.SCRAY_META_PORT))

  // memcached host
  val MEMCACHED_ENDPOINTS = ScrayProperties.getPropertyValue(PredefinedProperties.SCRAY_MEMCACHED_IPS)

  // scray seeds
  val SCRAY_SEEDS = ScrayProperties.getPropertyValue(PredefinedProperties.SCRAY_SEED_IPS)

  // expiration time of enpoint registrations
  val EXPIRATION = ScrayProperties.getPropertyValue(ScrayServicePropertiesRegistrar.SCRAY_ENDPOINT_LIFETIME)

  def inetAddr2EndpointString(iaddr: InetSocketAddress): String = s"${iaddr.getHostName}:${iaddr.getPort}"

  implicit def UUID2ScrayUUID(uuid: UUID): ScrayUUID =
    ScrayUUID(uuid.getLeastSignificantBits(), uuid.getMostSignificantBits())

  implicit def ScrayUUID2UUID(suuid: ScrayUUID): UUID =
    new UUID(suuid.leastSigBits, suuid.mostSigBits)

}
