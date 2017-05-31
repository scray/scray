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
package scray.loader.configparser

import com.datastax.driver.core.ConsistencyLevel
import com.twitter.util.Duration
import com.typesafe.scalalogging.LazyLogging
import java.net.{ InetAddress, InetSocketAddress }
import org.apache.commons.io.IOUtils
// scalastyle:off underscore.import
import org.parboiled2._
// scalastyle:on underscore.import
import scala.util.{ Failure, Try }
import scray.common.properties.predefined.PredefinedProperties
import scray.common.tools.ScrayCredentials
import scray.loader.{ CassandraHostsUndefinedException, JDBCURLUndefinedException }
import scray.loader.configuration.{ CassandraClusterConsistency, CassandraClusterCredentials, CassandraClusterDatacenter, 
  CassandraClusterHosts, CassandraClusterNameProperty, CassandraClusterProperties, CassandraClusterProperty, 
  DBMSConfigProperties, JDBCCredentialsProperty, JDBCProperties, JDBCProperty, JDBCURLProperty, MemcacheIps, 
  ScrayCompressionSize, ScrayEndpointLifetime, ScrayMetaPort, ScraySeedIps, ScrayServiceAdvertiseIP, ScrayServiceIp, 
  ScrayServiceOption, ScrayServicePort, ScrayServiceWriteDot }
import scray.core.service.properties.ScrayServicePropertiesRegistrar
import ScrayConfigurationParser.SeqToOption
import scray.loader.configuration.HDFSProperties
import scray.loader.configuration.HDFSProperty
import scray.loader.configuration.HDFSURLProperty
import scray.loader.configuration.HDFSCredentialsProperty
import scray.loader.HDFSURLUndefinedException

/**
 * A syntax parser for scray configuration files.
 * Configurations consist of both a file defining datastores and a set of files containing queryspace
 * configurations. Queryspaces will often be a accompanied with jar files containing the implementation
 * of merges etc.
 * 
 * The new syntax is much more cleary structured than the old properties and follows a simple grammar:
 *   ScrayConfiguration ::= Service Datastores+ QueryspaceURLs
 *   Service ::= "service" "{" ServiceSetting ("," ServiceSetting)* "}"
 *   ServiceSetting ::= "compressionsize" INT | "service" Host | "seed" Hosts | "memcache" Hosts | 
 *     "service" Port | "meta" Port | "advertise" Host | "lifetime" LONG TIMEUNIT | "writeDot" ("Y" | "N")
 *   Port ::= "port" INT
 *   Host ::= "host" STRING
 *   Hosts ::= "hosts" "(" STRING ("," STRING)* ")"
 *   Datastores ::= "connection" (ID)? (Cassandra | JDBC | HDFS)
 *   Cassandra ::= "cassandra" "{" (CassandraSetting ",")* CassandraSetting "}"
 *   CassandraSetting ::= "clustername" STRING | CassandraHostNames | "datacenter" STRING | Credentials
 *   CassandraHostNames ::= "hosts" "(" (STRING ",")* STRING ")"
 *   JDBC ::= "jdbc" "{" "url" STRING ("," Credentials)? "}"
 *   HDFS ::= "hdfs" "{" "url" STRING ("," Credentials)? "}"
 *   Credentials ::= "credentials" STRING ":" STRING
 *   QueryspaceURLs ::= "queryspacelocations" "{" (QueryspaceURL ",")* QueryspaceURL "}"
 *   QueryspaceURL ::= "url" STRING ("reload" ("never" | "all" INT "seconds"))?
 *   
 * The URL for the queryspaceURL can either be a directory or a file. If it is a directory it will
 * be scanned for files containing <filename>.config.scray (optionally there can be a file <filename>.jar).
 * If it is a file it will be re-read and there may be another file with <same name>.jar containing bytecode.
 *   
 * This grammar is intentionally not based on XML nor JSON, YAML or anything else which does not
 * have a clear and easy to use schema.
 */
// scalastyle:off method.name
// scalastyle:off number.of.methods
class ScrayConfigurationParser(override val input: ParserInput) extends ScrayGenericParsingRules with LazyLogging {

  /**
   * read until all input has been consumed
   */
  def InputLine: Rule1[ScrayConfiguration] = rule { ConfigModel ~ EOI }

  def ConfigModel: Rule1[ScrayConfiguration] = rule { ServiceOptions ~ oneOrMore(Datastores) ~ ConfigurationLocations ~> { 
    (serviceoptions: ScrayServiceOptions, stores: Seq[DBMSConfigProperties], urls: Seq[ScrayQueryspaceConfigurationURL]) => 
      ScrayConfiguration(serviceoptions, stores, urls) }}
  
  def Datastores: Rule1[DBMSConfigProperties] = rule { "connection" ~ optional(Identifier) ~ StoreTypes ~> {
    (name: Option[String], dbmsproperties: DBMSConfigProperties) => dbmsproperties.setName(name) }}
  
  def StoreTypes: Rule1[DBMSConfigProperties] = rule {  CassandraStoreConnection | JDBCStoreConnection | HDFSStoreConnection }
  
  /* -------------------------------- Cassandra connection rules ----------------------------------- */ 
  
  def CassandraStoreConnection: Rule1[CassandraClusterProperties] = rule { "cassandra" ~ BRACE_OPEN ~ oneOrMore(CassandraSetting).separatedBy(COMMA) ~ 
    BRACE_CLOSE ~> { (properties: Seq[CassandraClusterProperty]) =>
      properties.find(_.isInstanceOf[CassandraClusterHosts]).orElse(throw new CassandraHostsUndefinedException())
      properties.foldLeft(CassandraClusterProperties()) { (properties, entry) => entry match {
        case cname: CassandraClusterNameProperty => properties.copy(clusterName = cname.name)
        case hnames: CassandraClusterHosts => properties.copy(hosts = hnames.hosts)
        case creds: CassandraClusterCredentials => properties.copy(credentials = creds.credentials)
        case dc: CassandraClusterDatacenter => properties.copy(datacenter = dc.dc)
        case _ => properties
      }}
  }}
  
  def CassandraSetting: Rule1[CassandraClusterProperty] = rule { CassandraClusterName | CassandraHostNames | CassandraDatacenter | CassandraCredentials }
  
  def CassandraClusterName: Rule1[CassandraClusterNameProperty] = rule { "clustername" ~ QuotedString ~> { 
    (name: String) => CassandraClusterNameProperty(name) }}
  def CassandraHostNames: Rule1[CassandraClusterHosts] = rule { HostList ~> {
    (hosts: Seq[String]) => CassandraClusterHosts(hosts.toSet) }}
  def CassandraCredentials: Rule1[CassandraClusterCredentials] = rule { Credentials ~> {(creds: ScrayCredentials) => CassandraClusterCredentials(creds)}}
  def CassandraDatacenter: Rule1[CassandraClusterDatacenter] = rule { "datacenter" ~ QuotedString ~> { (dc: String) => CassandraClusterDatacenter(dc) }}
  def CassandraConsistency: Rule1[CassandraClusterConsistency] = rule { "consistency" ~ CassandraConsistencyLevel ~> { 
    (cl: ConsistencyLevel) => CassandraClusterConsistency(read = cl)} } 
  def CassandraConsistencyLevel: Rule1[ConsistencyLevel] = rule { Identifier ~> {(level: String) => level match {
      case "ANY" => ConsistencyLevel.ANY
      case "ONE" => ConsistencyLevel.ONE
      case "TWO" => ConsistencyLevel.TWO
      case "THREE" => ConsistencyLevel.THREE
      case "QUORUM" => ConsistencyLevel.QUORUM
      case "ALL" => ConsistencyLevel.ALL
      case "LOCAL_QUORUM" => ConsistencyLevel.LOCAL_QUORUM
      case "EACH_QUORUM" => ConsistencyLevel.EACH_QUORUM
      case "SERIAL" => ConsistencyLevel.SERIAL
      case "LOCAL_SERIAL" => ConsistencyLevel.LOCAL_SERIAL
      case "LOCAL_ONE" => ConsistencyLevel.LOCAL_ONE
      case _ =>
        logger.warn(s"Provided consistency level was not parsable. Using LOCAL_ONE instead. This might not be what you wanted.")
        ConsistencyLevel.LOCAL_ONE
    }}}
    
  /* -------------------------------- JDBC connection rules ----------------------------------- */
  
  def JDBCStoreConnection: Rule1[JDBCProperties] = rule { "jdbc" ~ BRACE_OPEN ~ oneOrMore(JDBCSetting).separatedBy(COMMA) ~ BRACE_CLOSE ~> {
    (properties: Seq[JDBCProperty]) =>
      val url = properties.find(_.isInstanceOf[JDBCURLProperty]).map(_.asInstanceOf[JDBCURLProperty]).getOrElse(throw new JDBCURLUndefinedException())
      properties.foldLeft(JDBCProperties(url.url)) { (properties, entry) => entry match {
        case creds: JDBCCredentialsProperty => properties.copy(credentials = creds.credentials)
        case _ => properties
      }}
  }}
  
  def JDBCSetting: Rule1[JDBCProperty] = rule { JDBCURL | JDBCCredentials } 
  
  def JDBCURL: Rule1[JDBCURLProperty] = rule { "url" ~ QuotedString ~> { (url: String) => JDBCURLProperty(url) }}
  def JDBCCredentials: Rule1[JDBCCredentialsProperty] = rule  { Credentials ~> {(creds: ScrayCredentials) => JDBCCredentialsProperty(creds)}}

  /* -------------------------------- HDFS connection rules ----------------------------------- */
  
  def HDFSStoreConnection: Rule1[HDFSProperties] = rule { "hdfs" ~ BRACE_OPEN ~ oneOrMore(HDFSSetting).separatedBy(COMMA) ~ BRACE_CLOSE ~> {
    (properties: Seq[HDFSProperty]) =>
      val url = properties.find(_.isInstanceOf[HDFSURLProperty]).map(_.asInstanceOf[HDFSURLProperty]).getOrElse(throw new HDFSURLUndefinedException())
      properties.foldLeft(HDFSProperties(url.url)) { (properties, entry) => entry match {
        case creds: HDFSCredentialsProperty => properties.copy(credentials = creds.credentials)
        case _ => properties
      }}
  }}
  
  def HDFSSetting: Rule1[HDFSProperty] = rule { HDFSURL | HDFSCredentials } 
  
  def HDFSURL: Rule1[HDFSURLProperty] = rule { "url" ~ QuotedString ~> { (url: String) => HDFSURLProperty(url) }}
  def HDFSCredentials: Rule1[HDFSCredentialsProperty] = rule  { Credentials ~> {(creds: ScrayCredentials) => HDFSCredentialsProperty(creds)}}

  
  /* -------------------------------- Queryspaces configuration location rules ----------------------------------- */
  
  def ConfigurationLocations: Rule1[Seq[ScrayQueryspaceConfigurationURL]] = rule { "queryspacelocations" ~ BRACE_OPEN ~ 
    oneOrMore(ConfigurationLocationSetting).separatedBy(COMMA) ~ BRACE_CLOSE} 

  def ConfigurationLocationSetting: Rule1[ScrayQueryspaceConfigurationURL] =
    rule { "url" ~ QuotedString ~ optional("reload" ~ ConfigurationLocationAutoreload) ~> { 
      (url: String, autoreload: Option[ScrayQueryspaceConfigurationURLReload]) => 
          ScrayQueryspaceConfigurationURL(url, autoreload.getOrElse(ScrayQueryspaceConfigurationURLReload())) }}
  def ConfigurationLocationAutoreload: Rule1[ScrayQueryspaceConfigurationURLReload] = 
    rule { ConfigurationLocationAutoreloadNever | ConfigurationLocationAutoreloadSeconds } 
  def ConfigurationLocationAutoreloadNever: Rule1[ScrayQueryspaceConfigurationURLReload] = 
    rule { capture("never") ~> { (_: String) => ScrayQueryspaceConfigurationURLReload(None) }}
  def ConfigurationLocationAutoreloadSeconds: Rule1[ScrayQueryspaceConfigurationURLReload] =
    rule { "all" ~ DurationRule ~> { (number: Duration) => ScrayQueryspaceConfigurationURLReload(Some(number)) }}
  
  /* -------------------------------- common connection rules ----------------------------------- */
  def Credentials: Rule1[ScrayCredentials] = rule { "credentials" ~ QuotedString ~ ":" ~ QuotedString ~> {
    (user: String, pwd: String) => new ScrayCredentials(user, pwd.toCharArray()) }}

  /* -------------------------------- Service option rules ----------------------------------- */
  def ServiceOptions: Rule1[ScrayServiceOptions] = rule { "service" ~ BRACE_OPEN ~ oneOrMore(ServiceOption).separatedBy(COMMA) ~ BRACE_CLOSE ~> {
    (options: Seq[ScrayServiceOption]) =>
      val compressionsize = options.collect {
        case size: ScrayCompressionSize => size.size
      }.toOption.getOrElse(PredefinedProperties.RESULT_COMPRESSION_MIN_SIZE.getDefault().toInt)
      val serviceip = options.collect {
        case sip: ScrayServiceIp => sip.ip
      }.toOption.getOrElse(InetAddress.getByName(PredefinedProperties.SCRAY_SERVICE_LISTENING_ADDRESS.getDefault()))
      val writeDot = options.collect {
        case dot: ScrayServiceWriteDot => dot.bool
      }.toOption.getOrElse(false)
      val scraySeedIps = options.collect {
        case sip: ScraySeedIps => sip.ips
      }.flatten
      val memcacheIps = options.collect {
        case sip: MemcacheIps => sip.ips
      }.flatten
      val serviceport = options.collect {
        case sip: ScrayServicePort => sip.port
      }.toOption.getOrElse(PredefinedProperties.SCRAY_QUERY_PORT.getDefault().toInt)
      val lifetime = options.collect {
        case time: ScrayEndpointLifetime => time.lifetime
      }.toOption.getOrElse(ScrayServicePropertiesRegistrar.SCRAY_ENDPOINT_LIFETIME.getDefault())
      val metaport = options.collect {
        case sip: ScrayMetaPort => sip.port
      }.toOption.getOrElse(PredefinedProperties.SCRAY_META_PORT.getDefault().toInt)
      val advertiseip = options.collect {
        case sip: ScrayServiceAdvertiseIP => sip.ip
      }.toOption.getOrElse { // bind to localhost only...  
        val localip = InetAddress.getLocalHost
        logger.warn(s"Warning: Scray service advertise address not set. Will advertise possibly wrong address: ${localip.getHostAddress()}")
        localip
      }
      ScrayServiceOptions(scraySeedIps.toSet, advertiseip, serviceip, compressionsize, memcacheIps.toSet, serviceport, metaport)
  }}
  def ServiceOption: Rule1[ScrayServiceOption] = rule { ServiceCompressionSize | ServiceIP | ServiceSeedIPs | 
    ServiceMemcacheIPs | ServicePort | ServiceMetaPort | ServiceAdvertiseIP | ServiceLifetime | ServiceWriteDot }
  
  def ServiceCompressionSize: Rule1[ScrayCompressionSize] = rule { "compressionsize" ~ IntNumber ~> { (size: Int) => ScrayCompressionSize(size) }}
  def ServiceIP: Rule1[ScrayServiceIp] = rule { "listening" ~ "host" ~ QuotedString ~> { (address: String) => ScrayServiceIp(InetAddress.getByName(address)) }}
  def ServiceSeedIPs: Rule1[ScraySeedIps] = rule { "seed" ~ HostAddressList ~> {(hosts: Seq[InetAddress]) => ScraySeedIps(hosts)}}
  def ServiceMemcacheIPs: Rule1[MemcacheIps] = rule { "memcache" ~ HostPortList ~> {(hosts: Seq[InetSocketAddress]) => MemcacheIps(hosts)}}
  def ServicePort: Rule1[ScrayServicePort] = rule { "service" ~ HostPort ~> {(port: Int) => ScrayServicePort(port)}}
  def ServiceMetaPort: Rule1[ScrayMetaPort] = rule { "meta" ~ HostPort ~> {(port: Int) => ScrayMetaPort(port)}}
  def ServiceAdvertiseIP: Rule1[ScrayServiceAdvertiseIP] = rule { "advertise" ~ "host" ~ QuotedString ~> { 
    (address: String) => ScrayServiceAdvertiseIP(InetAddress.getByName(address)) }}
  def ServiceLifetime: Rule1[ScrayEndpointLifetime] = rule { "lifetime" ~ DurationRule ~> { (duration: Duration) => ScrayEndpointLifetime(duration) }}
  def ServiceWriteDot: Rule1[ScrayServiceWriteDot] = rule { "writeDot" ~ BooleanRule ~> { (bool: Boolean) => ScrayServiceWriteDot(bool) }}

  def HostList: Rule1[Seq[String]] = rule { "hosts" ~ "(" ~ oneOrMore(QuotedString).separatedBy(COMMA) ~ ")" }
  def HostPort: Rule1[Int] = rule { "port" ~ IntNumber }
  def HostAddressList: Rule1[Seq[InetAddress]] = rule { HostList ~> { (hosts: Seq[String]) => 
    hosts.map(host => InetAddress.getByName(host)) }}
  def HostPortList: Rule1[Seq[InetSocketAddress]] = rule { HostList ~> { (hosts: Seq[String]) => 
    hosts.map { host => 
      val portDevider = host.lastIndexOf(":")
      val ip = if(portDevider > 0) {
        InetAddress.getByName(host.substring(0, portDevider))
      } else {
        InetAddress.getByName(host)
      }
      val port = if(portDevider > 0) {
        host.substring(portDevider).toInt
      } else {
        0
      }
      new InetSocketAddress(ip, port)
    }}}
}
// scalastyle:on number.of.methods
// scalastyle:on method.name

/**
 * companion brings methods to conveniently call the parser
 */
object ScrayConfigurationParser extends LazyLogging {
  implicit class SeqToOption[T](seq: Seq[T]) {
    def toOption: Option[T] = if (seq.size == 0) None else Some(seq(seq.size - 1))
  }
  
  private def handleWithErrorLogging(input: String, logError: Boolean = true): Try[ScrayConfiguration] = {
    val parser = new ScrayConfigurationParser(input)
    val parseResult = parser.InputLine.run()
    logError match {
      case true => parseResult.recoverWith { case e: ParseError =>
        val msg = parser.formatError(e)
        logger.error(s"Parse error parsing configuration file. Message from parser is $msg", e)
        Failure(e)
      }
      case false => parseResult
    }
  }
  def parse(text: String, logError: Boolean = true): Try[ScrayConfiguration] = handleWithErrorLogging(text, logError)
  def parseResource(resource: String, logError: Boolean = true): Try[ScrayConfiguration] = {
    val text = IOUtils.toString(this.getClass().getResourceAsStream(resource), "UTF-8")
    handleWithErrorLogging(text, logError)
  }
}
