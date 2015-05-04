package scray.core.service.spools.memcached

import org.junit.runner.RunWith
import org.mockito.Matchers.anyObject
import org.mockito.Mockito.when
import org.scalatest.FlatSpec
import org.scalatest.Matchers
import org.scalatest.mock.MockitoSugar
import com.twitter.concurrent.Spool
import scray.core.service.parser.TQueryParser
import scray.core.service.util.SpoolSamples
import scray.core.service.util.TQuerySamples
import scray.querying.Query
import scray.querying.description.Row
import org.scalatest.junit.JUnitRunner
import com.twitter.util.Duration
import com.twitter.util.JavaTimer
import scray.core.service.KryoPoolRegistration
import scray.common.serialization.KryoPoolSerialization
import scray.service.qmodel.thrifscala.ScrayTQuery
import scray.core.service.IntegrationTest
import scray.service.qmodel.thrifscala.ScrayTRow
import scray.core.service.spools.PageKey
import com.twitter.util.Await

/**
 * Integration test requires running memcached
 */
@RunWith(classOf[JUnitRunner])
class MemcachedPagesSpec
  extends FlatSpec
  with Matchers
  with MockitoSugar
  with TQuerySamples
  with SpoolSamples
  with KryoPoolRegistration {

  // kryo registration
  register

  // pagesize
  val PGSZ = 2

  // prepare back end (query engine) mock
  val mockplanner = mock[(Query) => Spool[Row]]
  when(mockplanner(anyObject())).thenReturn(spoolOf8.get)

  // prepare query related objects
  val tquery : ScrayTQuery = createTQuery(pagesize = PGSZ, buff = kryoStrbuff, expr = "SELECT @col1 FROM @myTableId")
  val parser = new TQueryParser(tquery)
  val query = parser.InputLine.run().get.createQuery.get
  val uuid = query.getQueryID

  "MemcachedPageRack" should "store pages in memcached" taggedAs (IntegrationTest) in {
    val rack = new MemcachedPageRack(planAndExecute = mockplanner)
    val updQInf = rack.createPages(query, tquery.queryInfo)
    val key0 = PageKey(updQInf.queryId.get, 0)
    // need to wait until pages are available in cache, otherwise we'll receive None
    try { Thread.sleep(50) } catch { case ex : InterruptedException => Thread.currentThread().interrupt() }
    val blockingPageValOption = rack.getPage(key0).get()
    println(s"PageKey:$key0 (${pidKeyEncoder(key0)}) PageValue:$blockingPageValOption")
    blockingPageValOption.get.page.nonEmpty should be(true)
  }

}
