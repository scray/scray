package scray.loader.tools

import org.junit.runner.RunWith
import com.typesafe.scalalogging.slf4j.LazyLogging
import org.scalatest.WordSpec
import org.scalatest.junit.JUnitRunner
import scray.loader.configparser.ScrayConfigurationParser
import scray.querying.description.TableIdentifier


@RunWith(classOf[JUnitRunner])
class QueryspaceConfigCreatorImplSpecs extends WordSpec with LazyLogging {
   "QueryspaceConfigCreator" should {
      "create header string " in {
      
    }
    "create table entry from tableidentifier " in {
       val configCreator = new QueryspaceConfigCreatorImpl("000", 1)
       val configurationString = configCreator.getConfig(List(TableIdentifier("", "", "")))
       
       ScrayConfigurationParser.parse(configurationString, false)
    }
   }
}