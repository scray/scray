package scray.loader.configparser

import com.typesafe.scalalogging.slf4j.LazyLogging
import org.apache.commons.io.IOUtils
import org.parboiled2._
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
class ScrayUserConfigurationParser (override val input: ParserInput, val config: ScrayConfiguration) 
    extends ScrayGenericParsingRules with LazyLogging {
  
  override implicit def wspStr(s: String): Rule0 = rule { str(s) ~ zeroOrMore(SingleLineWhitespaceChars) }
  
  /**
   * read until all input has been consumed
   */
  def InputLine = rule { UserConfigModel ~ EOI }

  def UserConfigModel: Rule1[ScrayUsersConfiguration] = rule { optional(LineBreak) ~ oneOrMore(UserLine) ~> { ScrayUsersConfiguration }}

  def UserLine: Rule1[ScrayAuthConfiguration] = rule { QuotedSingleString ~ ":" ~ AuthMethod ~ ":" ~ optional(QuotedSingleString) ~ ":" ~ 
    zeroOrMore(IdentifierSingle).separatedBy(",") ~ LineBreak ~> { (user: String, method: ScrayAuthMethod.Value, pwd: Option[String], qs: Seq[String]) =>
    ScrayAuthConfiguration(user, pwd.getOrElse(""), method, qs.toSet)
  }}

  def AuthMethod: Rule1[ScrayAuthMethod.Value] = rule { IdentifierSingle ~> { (id: String) => id.toUpperCase() match { 
    case "LDAP" => ScrayAuthMethod.LDAP
    case "PLAIN" => ScrayAuthMethod.Plain
    case _ => ScrayAuthMethod.Plain
  }}}
}

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
