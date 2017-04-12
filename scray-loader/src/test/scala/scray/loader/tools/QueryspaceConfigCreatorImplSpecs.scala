package scray.loader.tools

import org.junit.runner.RunWith
import com.typesafe.scalalogging.slf4j.LazyLogging
import org.scalatest.WordSpec
import org.scalatest.junit.JUnitRunner
import scray.querying.description.TableIdentifier
import scray.loader.configparser.ScrayConfigurationParser


@RunWith(classOf[JUnitRunner])
class QueryspaceConfigCreatorImplSpecs extends WordSpec with LazyLogging {
   "QueryspaceConfigCreator" should {
      "create header string " in {
        val configCreator = new QueryspaceConfigCreatorImpl("000", 1)
        assert(configCreator.getHeader.trim() == "name 000 version 1")
    }
    "create table entry from tableidentifier " in {
       val configCreator = new QueryspaceConfigCreatorImpl("000", 1)
       val configurationString = configCreator.getConfig(List(TableIdentifier("cassandra", "ks", "cf1")))
       println(configurationString)
       ScrayConfigurationParser.parse(configurationString, false)
    }
   }
}