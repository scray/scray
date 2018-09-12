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

import com.typesafe.scalalogging.LazyLogging
import org.apache.commons.io.IOUtils
// scalastyle:off underscore.import
import org.parboiled2._
// scalastyle:on underscore.import
import scala.util.{ Failure, Try }
import scray.loader.{ DBMSUndefinedException, UnsupportedMappingTypeException }
import scray.loader.configuration.{ QueryspaceIndexstore, QueryspaceOption, QueryspaceRowstore, QueryspaceMaterializedView }
import scray.querying.description.TableIdentifier


/**
 * Parse properties for a queryspace and build a configuration object.
 * Grammar ::= Name (Synctable)? (Tables)*
 * Name ::= "name" ID
 * Synctable ::= "sync" "table" "{" ID "," STRING "," STRING "}" 
 * Tables ::= Rowstore | Indexstore
 * Rowstore ::= Tableid
 * Indexstore ::= "manualindex" "{" "type" ("time" | "wildcard") "," Tableid 
 *                "indexedcolumn" STRING "," "index" STRING ("," "mapping" ID "->" ID)? "}"
 * Tableid ::= "table" "{" ID "," STRING "," STRING "}"
 * Materialized_View ::= Materialized_View "{" ID "," STRING "," STRING "}[" STRING "]"
 * TODO: implement materialized view configuration
 * 
 */
// scalastyle:off method.name
class ScrayQueryspaceConfigurationParser (override val input: ParserInput, val config: ScrayConfiguration) 
    extends ScrayGenericParsingRules with LazyLogging {
  
  var name: Option[String] = None
  
  /**
   * read until all input has been consumed
   */
  def InputLine: Rule1[ScrayQueryspaceConfiguration] = rule { QueryspaceConfigModel ~ EOI }

  def QueryspaceConfigModel: Rule1[ScrayQueryspaceConfiguration] = rule { zeroOrMore(Comment) ~ QueryspaceName ~ zeroOrMore(Comment) ~ optional(SyncTable) ~ zeroOrMore(Comment) ~ zeroOrMore(ConfigurationOptions) ~> {
    (name: (String, Long), syncTable: Option[TableIdentifier], options : Seq[QueryspaceOption]) =>
      val rowstores = options.collect {
        case row: QueryspaceRowstore => row.table
      }
      val indexstores = options.collect {
        case index: QueryspaceIndexstore => index
      }
      val materializedViews = options.collect {
        case mv: QueryspaceMaterializedView => mv
      }
      
      ScrayQueryspaceConfiguration(name._1, name._2, syncTable, rowstores, indexstores, materializedViews)
  }}

  /**
   * set the queryspaces name attribute
   */
  def QueryspaceName: Rule1[(String, Long)] = rule { "name" ~ Identifier ~ "version" ~ LongNumber ~> { (id: String, version: Long) => 
    name = Some(id)
    (id, version)
  }}
  
  def SyncTable: Rule1[TableIdentifier] = rule { "sync" ~ RowStore ~> { (table: QueryspaceRowstore) => table.table }}
  
  def ConfigurationOptions: Rule1[QueryspaceOption] = rule {  zeroOrMore(Comment) ~ ( RowStore | IndexStore | MaterializedView) ~ zeroOrMore(Comment)}
  
  def RowStore: Rule1[QueryspaceRowstore] = rule { "table" ~ "{" ~ Identifier ~ COMMA ~ QuotedString ~ COMMA ~ QuotedString ~ "}" ~> { 
    (dbms: String, dbid: String, table: String) => 
      // check that the given identifier has been defined previously
      config.stores.find { _.getName == dbms }.orElse(throw new DBMSUndefinedException(dbms, name.get))
      QueryspaceRowstore(TableIdentifier(dbms, dbid, table)) 
  }}

  def MaterializedView: Rule1[QueryspaceMaterializedView] = rule {"materialized_view" ~ "table" ~ "{" ~ Identifier ~ COMMA ~ QuotedString ~ COMMA ~ QuotedString ~ "}" ~ optional(COMMA ~ "keygeneratorClass:" ~ QuotedString) ~>  {
    (dbms: String, dbid: String, table: String, keygenerator: Option[String]) => new QueryspaceMaterializedView(TableIdentifier(dbms, dbid, table), keygenerator.getOrElse(""))
   }}
  
  def IndexStore: Rule1[QueryspaceIndexstore] = rule { "manualindex" ~ "{" ~ 
    "type" ~ IndexType ~ COMMA ~ 
    RowStore ~ "indexedcolumn" ~ QuotedString ~ COMMA ~ 
    "index" ~ QuotedString ~ optional(MappingType) ~ "}" ~> { 
      (indextype: String, table: QueryspaceRowstore, columnname: String, indexjobid: String, mapping: Option[String]) =>
      QueryspaceIndexstore(indextype, table.table, columnname, indexjobid, mapping)
    }
  }
    
  def IndexType: Rule1[String] = rule { capture("time") | capture("wildcard") }

  def MappingType: Rule1[String] = rule { COMMA ~ "mapping" ~ Identifier ~ "->" ~ Identifier ~> { (in: String, out: String) => 
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
// scalastyle:on method.name

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
