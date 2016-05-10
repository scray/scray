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

import com.typesafe.scalalogging.slf4j.LazyLogging
import org.apache.commons.io.IOUtils
// scalastyle:off underscore.import
import org.parboiled2._
// scalastyle:on underscore.import
import scala.util.{ Failure, Try }
import scray.loader.{ DBMSUndefinedException, UnsupportedMappingTypeException }
import scray.loader.configuration.{ QueryspaceIndexstore, QueryspaceOption, QueryspaceRowstore }
import scray.querying.description.TableIdentifier

/**
 * Parse properties for a user model with Queryspaces.
 * Grammar ::= (STRING ":" STRING ":" ("LDAP" | "PLAIN") ":" ID? ("," ID)* "\n")*
 * Meaning that there may one or more lines containing username and password pairs
 * along with the information about the directory identification and a comma-separated
 * list of queryspace-names.
 */
// scalastyle:off method.name
class ScrayUserConfigurationParser (override val input: ParserInput, val config: ScrayConfiguration) 
    extends ScrayGenericParsingRules with LazyLogging {
  
  override implicit def wspStr(s: String): Rule0 = rule { str(s) ~ zeroOrMore(SingleLineWhitespaceChars) }
  
  /**
   * read until all input has been consumed
   */
  def InputLine: Rule1[ScrayUsersConfiguration] = rule { UserConfigModel ~ EOI }

  def UserConfigModel: Rule1[ScrayUsersConfiguration] = rule { optional(LineBreak) ~ oneOrMore(UserLine) ~> { ScrayUsersConfiguration }}

  def UserLine: Rule1[ScrayAuthConfiguration] = rule { QuotedSingleString ~ COLON ~ AuthMethod ~ COLON ~ optional(QuotedSingleString) ~ COLON ~ 
    zeroOrMore(IdentifierSingle).separatedBy(",") ~ LineBreak ~> { (user: String, method: ScrayAuthMethod.Value, pwd: Option[String], qs: Seq[String]) =>
    ScrayAuthConfiguration(user, pwd.getOrElse(""), method, qs.toSet)
  }}

  def AuthMethod: Rule1[ScrayAuthMethod.Value] = rule { IdentifierSingle ~> { (id: String) => id.toUpperCase() match { 
    case "LDAP" => ScrayAuthMethod.LDAP
    case "PLAIN" => ScrayAuthMethod.Plain
    case _ => ScrayAuthMethod.Plain
  }}}
}
// scalastyle:on method.name

object ScrayUserConfigurationParser extends LazyLogging {
  private def handleWithErrorLogging(input: String, config: ScrayConfiguration, logError: Boolean = true): Try[ScrayUsersConfiguration] = {
    val parser = new ScrayUserConfigurationParser(input, config)
    val parseResult = parser.InputLine.run()
    logError match {
      case true => parseResult.recoverWith { case e: ParseError =>
        val msg = parser.formatError(e)
        logger.error(s"Parse error parsing user-configuration file. Message from parser is $msg", e)
        Failure(e)
      }
      case false => parseResult
    }
  }
  def parse(text: String, config: ScrayConfiguration, logError: Boolean = true): Try[ScrayUsersConfiguration] =
    handleWithErrorLogging(text, config, logError)
  def parseResource(resource: String, config: ScrayConfiguration, logError: Boolean = true): Try[ScrayUsersConfiguration] = {
    val text = IOUtils.toString(this.getClass().getResourceAsStream(resource), "UTF-8")
    handleWithErrorLogging(text, config, logError)
  }
}
