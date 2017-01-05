package scray.loader.configparser

import org.apache.commons.io.IOUtils
import org.junit.runner.RunWith
import org.parboiled2.ParseError
import org.parboiled2.ParserInput.apply
import org.parboiled2.Rule.Runnable
import org.scalatest.Finders
import org.scalatest.WordSpec
import org.scalatest.junit.JUnitRunner
import scray.common.tools.ScrayCredentials
import scray.loader.configuration.CassandraClusterProperties
import com.typesafe.scalalogging.slf4j.LazyLogging
import com.twitter.util.{Try, Throw}
import org.parboiled2._
import scray.loader.CassandraHostsUndefinedException
import scray.loader.configuration.JDBCProperties
import scray.loader.JDBCURLUndefinedException
import scray.querying.description.TableIdentifier
import scray.loader.configparser.ScrayUserConfigurationParser

/**
 * Scray loader parser specification.
 */
@RunWith(classOf[JUnitRunner])
class ScrayConfigurationParserSpecs extends WordSpec with LazyLogging {
  "Scray's configuration parser" should {
    "throw on an empty config file" in {
      intercept[ParseError] {
        val file = ScrayConfigurationParser.parseResource("/configs/scrayemptyconfig1.txt", false)
        file.get
      }
    }
    "load test cassandra store config" in {
      val result = ScrayConfigurationParser.parseResource("/configs/scraycassandraconfig1.txt")
      assert(result.get.stores.size > 0)
      assert(result.get.stores(0).isInstanceOf[CassandraClusterProperties])
      val ccp = result.get.stores(0).asInstanceOf[CassandraClusterProperties]
      assert(ccp.clusterName === "Test Cluster")
      assert(ccp.hosts.size === 3)
      assert(ccp.datacenter === "DC1001")
      assert(ccp.credentials === new ScrayCredentials("test", "closed".toCharArray()))
    }
    "throw on missing host config for cassandra store config" in {
      intercept[CassandraHostsUndefinedException] {
        val result = ScrayConfigurationParser.parseResource("/configs/scraycassandraconfig2.txt")
        result.get
      }
    }
    "load test jdbc store config" in {
      val result = ScrayConfigurationParser.parseResource("/configs/scrayjdbcconfig1.txt")
      assert(result.get.stores.size > 0)
      assert(result.get.stores(0).isInstanceOf[JDBCProperties])
      val jsp = result.get.stores(0).asInstanceOf[JDBCProperties]
      assert(jsp.url === "jdbc:oracle:thin:blaschwaetz")
      assert(jsp.credentials === new ScrayCredentials("test", "closed".toCharArray()))
    }
    "throw on missing url config for jdbc store config" in {
      intercept[JDBCURLUndefinedException] {
        val result = ScrayConfigurationParser.parseResource("/configs/scrayjdbcconfig2.txt")
        result.get
      }
    }
    "load test queryspace config with 1 url" in {
      val result = ScrayConfigurationParser.parseResource("/configs/scrayqstestconfig1.txt")
      val urls = result.get.urls
      assert(urls.size == 1)
      assert(urls(0).reload === ScrayQueryspaceConfigurationURLReload(Some(ScrayQueryspaceConfigurationURLReload.DEFAULT_URL_RELOAD)))
      assert(urls(0).url.length() > 0)
    }
    "load test queryspace config with 2 urls" in {
      val result = ScrayConfigurationParser.parseResource("/configs/scrayqstestconfig2.txt")
      val urls = result.get.urls
      assert(urls.size == 2)
      assert(urls(0).reload === ScrayQueryspaceConfigurationURLReload(Some(ScrayQueryspaceConfigurationURLReload.DEFAULT_URL_RELOAD)))
      assert(urls(0).url.length() > 0)
      assert(urls(0).url.substring(0, urls(0).url.length() - 1) === urls(1).url.substring(0, urls(0).url.length() - 1))
    }
  }
  "Scray's queryspace configuration parser" should {
    "throw on an empty config file" in {
      intercept[ParseError] {
        val config = ScrayConfigurationParser.parseResource("/configs/scrayjdbcconfig1.txt")
        val file = ScrayQueryspaceConfigurationParser.parseResource("/configs/scrayemptyconfig1.txt", config.get, false)
        file.get
      }
    }
    "load test minimalistic queryspace store config1" in {
      val config = ScrayConfigurationParser.parseResource("/configs/scrayjdbcconfig1.txt")
      val result = ScrayQueryspaceConfigurationParser.parseResource("/configs/queryspaceconfig1.txt", config.get, false)
      assert(result.get.name === "WhateverYouLike")
    }
    "load test queryspace config with row-store config2" in {
      val config = ScrayConfigurationParser.parseResource("/configs/scrayjdbcconfig1.txt")
      val result = ScrayQueryspaceConfigurationParser.parseResource("/configs/queryspaceconfig2.txt", config.get, false)
      assert(result.get.name === "WhateverYouLike")
      assert(result.get.rowStores.size > 0)
      assert(result.get.rowStores(0) === TableIdentifier("test", "BLA", "SCHWAETZ"))      
    }
    "load test queryspace config with idnex-store config3" in {
      val config = ScrayConfigurationParser.parseResource("/configs/scraymulticonfig1.txt")
      val result = ScrayQueryspaceConfigurationParser.parseResource("/configs/queryspaceconfig3.txt", config.get, false)
      assert(result.get.name === "WhateverYouLike")
      assert(result.get.syncTable === Some(TableIdentifier("test1", "IDX", "SyncTable")))
      assert(result.get.rowStores.size == 3)
      assert(result.get.rowStores(0) === TableIdentifier("test1", "BLA1", "SCHWAETZ1"))
      assert(result.get.rowStores(1) === TableIdentifier("test1", "BLUBB2", "SCHWAETZ1"))
      assert(result.get.rowStores(2) === TableIdentifier("test2", "BRUMM", "SCHWAETZ2"))
      assert(result.get.indexStores.size == 2)
      assert(result.get.indexStores(0).indextype === "time")
      assert(result.get.indexStores(0).table === TableIdentifier("test1", "BLA1", "SCHWAETZ1"))
      assert(result.get.indexStores(0).column === "indexedcol")
      assert(result.get.indexStores(0).indexjobid === "myjobid")
      assert(result.get.indexStores(1).indextype === "time")
      assert(result.get.indexStores(1).table === TableIdentifier("test1", "BLUBB2", "SCHWAETZ1"))
      assert(result.get.indexStores(1).column === "indexedcol2")
      assert(result.get.indexStores(1).indexjobid === "myfobid")
      assert(result.get.indexStores(1).mapping.get === "UUID->TEXT")
    }
    "load materialized view queryspace config" in {
      val config = ScrayConfigurationParser.parseResource("/configs/scrayjdbcconfig1.txt")
      val result = ScrayQueryspaceConfigurationParser.parseResource("/configs/queryspaceconfigMv.txt", config.get, false)
      assert(result.get.name === "WhateverYouLike")
      assert(result.get.materializedViews.size == 1)
      assert(result.get.materializedViews.head.table === TableIdentifier("cassandra", "KS1", "MV1"))
    }
  }
  "Scray's user configuration parser" should {
    "throw on an empty config file" in {
      intercept[ParseError] {
        val config = ScrayConfigurationParser.parseResource("/configs/scrayjdbcconfig1.txt")
        val file = ScrayUserConfigurationParser.parseResource("/configs/scrayemptyconfig1.txt", config.get, false)
        file.get
      }
    }
    "load test minimalistic user config 0" in {
      val config = ScrayConfigurationParser.parseResource("/configs/scrayjdbcconfig1.txt")
      val result = ScrayUserConfigurationParser.parseResource("/configs/usertest0.txt", config.get, false)
      assert(result.get.users.size == 1)
      assert(result.get.users.find { x => x.user == "Barack" }.isDefined)
    }
    "load test user config 1" in {
      val config = ScrayConfigurationParser.parseResource("/configs/scrayjdbcconfig1.txt")
      val result = ScrayUserConfigurationParser.parseResource("/configs/usertest1.txt", config.get, false)
      assert(result.get.users.size == 4)
      assert(result.get.users.find { x => x.user == "Angela" }.isDefined)
      assert(result.get.users.find { x => x.user == "Angela" }.get.pwd === "Merkel")
      assert(result.get.users.find { x => x.user == "Angela" }.get.method === ScrayAuthMethod.LDAP)
      assert(result.get.users.find { x => x.user == "Angela" }.get.queryspaces === Set("test1", "test2"))
    }
  }
}
