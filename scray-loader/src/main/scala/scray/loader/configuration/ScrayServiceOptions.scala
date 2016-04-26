package scray.loader.configuration

import com.twitter.util.Duration
import java.net.{ InetAddress, InetSocketAddress }

trait ScrayServiceOption
case class ScrayCompressionSize(size: Int) extends ScrayServiceOption
case class ScrayServiceIp(ip: InetAddress) extends ScrayServiceOption 
case class ScraySeedIps(ips: Seq[InetAddress]) extends ScrayServiceOption 
case class MemcacheIps(ips: Seq[InetSocketAddress]) extends ScrayServiceOption 
case class ScrayServicePort(port: Int) extends ScrayServiceOption 
case class ScrayMetaPort(port: Int) extends ScrayServiceOption
case class ScrayServiceAdvertiseIP(ip: InetAddress) extends ScrayServiceOption
case class ScrayEndpointLifetime(lifetime: Duration) extends ScrayServiceOption
case class ScrayServiceWriteDot(bool: Boolean) extends ScrayServiceOption