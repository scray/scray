package scray.loader.configparser

import org.parboiled2._
import com.typesafe.scalalogging.slf4j.LazyLogging
import scray.querying.description.TableIdentifier
import scray.loader.configuration.QueryspaceOption
import scray.loader.configuration.QueryspaceRowstore
import scray.loader.DBMSUndefinedException
import shapeless._
import scray.loader.UnsupportedMappingTypeException
import scray.loader.configuration.QueryspaceIndexstore
import scala.util.Try
import scala.util.Failure
import org.apache.commons.io.IOUtils

/**
 * Parse properties for a queryspace and build a configuration object.
 * Grammar ::= Name (Synctable)? (Tables)*
 * Name ::= "name" ID
 * Synctable ::= "sync" "table" "{" ID "," STRING "," STRING "}" 
 * Tables ::= Rowstore | Indexstore
 * Rowstore ::= Tableid
 * Indexstore ::= "manualindex" "{" "type" ("time" | "wildcard") "," Tableid 
 * 								"indexedcolumn" STRING "," "index" STRING ("," "mapping" ID "->" ID)? "}"
 * Tableid ::= "table" "{" ID "," STRING "," STRING "}"
 * TODO: implement materialized view configuration
 * 
 */
class ScrayQueryspaceConfigurationParser (override val input: ParserInput, val config: ScrayConfiguration) 
    extends ScrayGenericParsingRules with LazyLogging {
  
  var name: Option[String] = None
  
  /**
   * read until all input has been consumed
   */
  def InputLine = rule { QueryspaceConfigModel ~ EOI }

  def QueryspaceConfigModel: Rule1[ScrayQueryspaceConfiguration] = rule { QueryspaceName ~ optional(SyncTable) ~ zeroOrMore(ConfigurationOptions) ~> {
    (name: (String, Long), syncTable: Option[TableIdentifier], options : Seq[QueryspaceOption]) =>
      val rowstores = options.collect {
        case row: QueryspaceRowstore => row.table
      }
      val indexstores = options.collect {
        case index: QueryspaceIndexstore => index
      }
      ScrayQueryspaceConfiguration(name._1, name._2, syncTable, rowstores, indexstores)
  }}

  /**
   * set the queryspaces name attribute
   */
  def QueryspaceName: Rule1[(String, Long)] = rule { "name" ~ Identifier ~ "version" ~ LongNumber ~> { (id: String, version: Long) => 
    name = Some(id)
    (id, version)
  }}
  
  def SyncTable: Rule1[TableIdentifier] = rule { "sync" ~ RowStore ~> { (table: QueryspaceRowstore) => table.table }}
  
  def ConfigurationOptions: Rule1[QueryspaceOption] = rule { RowStore | IndexStore }
  
  def RowStore: Rule1[QueryspaceRowstore] = rule { "table" ~ "{" ~ Identifier ~ "," ~ QuotedString ~ "," ~ QuotedString ~ "}" ~> { (dbms: String, dbid: String, table: String) => 
    // check that the given identifier has been defined previously
    config.stores.find { _.getName == dbms }.orElse(throw new DBMSUndefinedException(dbms, name.get))
    QueryspaceRowstore(TableIdentifier(dbms, dbid, table)) }}
  
  def IndexStore: Rule1[QueryspaceIndexstore] = rule { "manualindex" ~ "{" ~ 
    "type" ~ IndexType ~ "," ~ 
    RowStore ~ "indexedcolumn" ~ QuotedString ~ "," ~ 
    "index" ~ QuotedString ~ optional(MappingType) ~ "}" ~> { 
      (indextype: String, table: QueryspaceRowstore, columnname: String, indexjobid: String, mapping: Option[String]) =>
      QueryspaceIndexstore(indextype, table.table, columnname, indexjobid, mapping)
    }
  }
    
  def IndexType: Rule1[String] = rule { capture("time") | capture("wildcard") }

  def MappingType: Rule1[String] = rule { "," ~ "mapping" ~ Identifier ~ "->" ~ Identifier ~> { (in: String, out: String) => 
    def checkSupportedMappingType(typ: String, mapping: String): Unit = typ match {
      case "UUID" =>
      case "TEXT" =>
      case _ => throw new UnsupportedMappingTypeException(mapping, name.get)
    }
    val result = in.trim() + "->" + out.trim()
    checkSupportedMappingType(in.trim(), result)
    checkSupportedMappingType(out.trim(), result)
    result
  }}  
}

object ScrayQueryspaceConfigurationParser extends LazyLogging {
  private def handleWithErrorLogging(input: String, config: ScrayConfiguration, logError: Boolean = true): Try[ScrayQueryspaceConfiguration] = {
    val parser = new ScrayQueryspaceConfigurationParser(input, config)
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
  def parse(text: String, config: ScrayConfiguration, logError: Boolean = true): Try[ScrayQueryspaceConfiguration] =
    handleWithErrorLogging(text, config, logError)
  def parseResource(resource: String, config: ScrayConfiguration, logError: Boolean = true): Try[ScrayQueryspaceConfiguration] = {
    val text = IOUtils.toString(this.getClass().getResourceAsStream(resource), "UTF-8")
    handleWithErrorLogging(text, config, logError)
  }
}
