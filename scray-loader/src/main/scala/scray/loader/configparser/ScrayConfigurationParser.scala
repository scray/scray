package scray.loader.configparser

import org.parboiled2._
import com.typesafe.scalalogging.slf4j.LazyLogging
import scray.loader.configuration.DBMSConfigProperties
import scray.loader.configuration.CassandraClusterProperties
import scray.loader.configuration.CassandraClusterNameProperty
import scray.loader.configuration.CassandraClusterHosts
import scray.loader.configuration.CassandraClusterProperty
import scray.loader.configuration.CassandraClusterDatacenter
import scray.loader.configuration.CassandraClusterCredentials
import scray.common.tools.ScrayCredentials
import scray.loader.configuration.JDBCProperty
import scray.loader.configuration.JDBCURLProperty
import scray.loader.configuration.JDBCCredentialsProperty
import com.twitter.storehaus.cassandra.cql.CQLCassandraConfiguration.StoreHost
import scray.loader.CassandraHostsUndefinedException
import scray.loader.configuration.JDBCProperties
import scala.util.Try
import org.apache.commons.io.IOUtils
import scala.util.Failure
import scray.loader.JDBCURLUndefinedException

/**
 * A syntax parser for scray configuration files.
 * Configurations consist of both a file defining datastores and a set of files containing queryspace
 * configurations. Queryspaces will often be a accompanied with jar files containing the implementation
 * of merges etc.
 * 
 * The new syntax is much more cleary structured than the old properties and follows a simple grammar:
 *   ScrayConfiguration ::= Datastores+ QueryspaceURLs
 *   Datastores ::= "connection" (ID)? (Cassandra | JDBC)
 *   Cassandra ::= "cassandra" "{" (CassandraSetting ",")* CassandraSetting "}"
 *   CassandraSetting ::= "clustername" STRING | CassandraHostNames | "datacenter" STRING | Credentials
 *   CassandraHostNames ::= "hosts" "(" (STRING ",")* STRING ")"
 *   JDBC ::= "jdbc" "{" "url" STRING ("," Credentials)? "}"
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
class ScrayConfigurationParser(override val input: ParserInput) extends ScrayGenericParsingRules with LazyLogging {
  
  /**
   * read until all input has been consumed
   */
  def InputLine = rule { ConfigModel ~ EOI }

  def ConfigModel: Rule1[ScrayConfiguration] = rule { oneOrMore(Datastores) ~ ConfigurationLocations ~> { 
    (stores: Seq[DBMSConfigProperties], urls: Seq[ScrayQueryspaceConfigurationURL]) => ScrayConfiguration(stores, urls) }}
  
  def Datastores: Rule1[DBMSConfigProperties] = rule { "connection" ~ optional(Identifier) ~ StoreTypes ~> {
    (name: Option[String], dbmsproperties: DBMSConfigProperties) => dbmsproperties.setName(name) }}
  
  def StoreTypes: Rule1[DBMSConfigProperties] = rule {  CassandraStoreConnection | JDBCStoreConnection }
  
  /* -------------------------------- Cassandra connection rules ----------------------------------- */ 
  
  def CassandraStoreConnection: Rule1[CassandraClusterProperties] = rule { "cassandra" ~ "{" ~ oneOrMore(CassandraSetting).separatedBy(",") ~ "}" ~> {
    (properties: Seq[CassandraClusterProperty]) =>
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
  
  def CassandraClusterName: Rule1[CassandraClusterNameProperty] = rule { "clustername" ~ QuotedString ~> { (name: String) => CassandraClusterNameProperty(name) }}
  def CassandraHostNames: Rule1[CassandraClusterHosts] = rule { "hosts" ~ "(" ~ oneOrMore(QuotedString).separatedBy(",") ~ ")" ~> {
    (hosts: Seq[String]) => CassandraClusterHosts(hosts.map(host => StoreHost(host)).toSet) }}
  def CassandraCredentials: Rule1[CassandraClusterCredentials] = rule { Credentials ~> {(creds: ScrayCredentials) => CassandraClusterCredentials(creds)}}
  def CassandraDatacenter: Rule1[CassandraClusterDatacenter] = rule { "datacenter" ~ QuotedString ~> { (dc: String) => CassandraClusterDatacenter(dc) }}
  
  /* -------------------------------- JDBC connection rules ----------------------------------- */
  
  def JDBCStoreConnection: Rule1[JDBCProperties] = rule { "jdbc" ~ "{" ~ oneOrMore(JDBCSetting).separatedBy(",") ~ "}" ~> {
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
  
  /* -------------------------------- Queryspaces configuration location rules ----------------------------------- */
  
  def ConfigurationLocations: Rule1[Seq[ScrayQueryspaceConfigurationURL]] = rule { "queryspacelocations" ~ "{" ~ oneOrMore(ConfigurationLocationSetting).separatedBy(",") ~ "}"} 

  def ConfigurationLocationSetting: Rule1[ScrayQueryspaceConfigurationURL] =
    rule { "url" ~ QuotedString ~ optional("reload" ~ ConfigurationLocationAutoreload) ~> { (url: String, autoreload: Option[ScrayQueryspaceConfigurationURLReload]) => 
      ScrayQueryspaceConfigurationURL(url, autoreload.getOrElse(ScrayQueryspaceConfigurationURLReload())) }}
  def ConfigurationLocationAutoreload: Rule1[ScrayQueryspaceConfigurationURLReload] = 
    rule { ConfigurationLocationAutoreloadNever | ConfigurationLocationAutoreloadSeconds } 
  def ConfigurationLocationAutoreloadNever: Rule1[ScrayQueryspaceConfigurationURLReload] = 
    rule { capture("never") ~> { (_: String) => ScrayQueryspaceConfigurationURLReload(None) }}
  def ConfigurationLocationAutoreloadSeconds: Rule1[ScrayQueryspaceConfigurationURLReload] =
    rule { "all" ~ IntNumber ~ "seconds" ~> { (number: Int) => ScrayQueryspaceConfigurationURLReload(Some(number)) }}
  
  /* -------------------------------- common connection rules ----------------------------------- */
  def Credentials: Rule1[ScrayCredentials] = rule { "credentials" ~ QuotedString ~ ":" ~ QuotedString ~> {
    (user: String, pwd: String) => new ScrayCredentials(user, pwd.toCharArray()) }}
  
}

/**
 * companion brings methods to conveniently call the parser
 */
object ScrayConfigurationParser extends LazyLogging {
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