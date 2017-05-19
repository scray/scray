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
package scray.loader.configuration

import com.twitter.util.Duration
import java.net.{ InetAddress, InetSocketAddress }

trait ScrayServiceOption
case class ScrayServiceRereadInterval(interval: Int) extends ScrayServiceOption
case class ScrayCompressionSize(size: Int) extends ScrayServiceOption
case class ScrayServiceIp(ip: InetAddress) extends ScrayServiceOption 
case class ScraySeedIps(ips: Seq[InetAddress]) extends ScrayServiceOption 
case class MemcacheIps(ips: Seq[InetSocketAddress]) extends ScrayServiceOption 
case class ScrayServicePort(port: Int) extends ScrayServiceOption 
case class ScrayMetaPort(port: Int) extends ScrayServiceOption
case class ScrayServiceAdvertiseIP(ip: InetAddress) extends ScrayServiceOption
case class ScrayEndpointLifetime(lifetime: Duration) extends ScrayServiceOption
case class ScrayServiceWriteDot(bool: Boolean) extends ScrayServiceOption
