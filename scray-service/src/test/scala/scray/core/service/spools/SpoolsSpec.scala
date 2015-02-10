package scray.core.service.spools

import java.util.UUID
import org.junit.runner.RunWith
import org.mockito.Matchers.anyObject
import org.mockito.Mockito.when
import org.scalatest.FlatSpec
import org.scalatest.Matchers
import org.scalatest.junit.JUnitRunner
import org.scalatest.mock.MockitoSugar
import com.twitter.bijection.Bijection
import com.twitter.chill.KryoInjection
import com.twitter.concurrent.Spool
import com.twitter.util.Duration
import com.twitter.util.JavaTimer
import scray.common.serialization.KryoPoolSerialization
import scray.core.service._
import scray.core.service.KryoPoolRegistration
import scray.core.service.parser.TQueryParser
import scray.core.service.util.SpoolSamples
import scray.core.service.util.TQuerySamples
import scray.querying.Query
import scray.querying.description.Row
import scray.service.qmodel.thrifscala.ScrayTQuery
import scray.service.qmodel.thrifscala.ScrayTRow
import org.xerial.snappy.Snappy
import scray.common.properties.predefined.PredefinedProperties

@RunWith(classOf[JUnitRunner])
class SpoolsSpec
  extends FlatSpec
  with Matchers
  with MockitoSugar
  with TQuerySamples
  with SpoolSamples
  with KryoPoolRegistration {

  registerProperties
  
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

  "VersionedSpoolRack" should "do CRUD on spools" in {
    val rack = new VersionedSpoolRack(planAndExecute = mockplanner)
    rack.createSpool(query, tquery.queryInfo)
    val spool = rack.getSpool(uuid)
    spool.nonEmpty should be(true)
    spool.get.spool shouldBe a[Spool[_]]
    rack.removeSpool(uuid)
    rack.getSpool(uuid).isEmpty should be(true)
  }

  it should "remove spool entries after ttl" in {
    val rack = new VersionedSpoolRack(ttl = Duration.fromMilliseconds(10), planAndExecute = mockplanner)
    val timer = new JavaTimer(true)
    rack.createSpool(query, tquery.queryInfo)
    timer.doLater(Duration.fromMilliseconds(20))(rack.getSpool(uuid).isEmpty should be(true))
  }

  it should "alter ttl with updates" in {
    import concurrent._
    import ExecutionContext.Implicits._
    import duration._
    val rack = new VersionedSpoolRack(planAndExecute = mockplanner)
    val qinf1 = rack createSpool (query, tquery.queryInfo)
    val spool0 = rack.getSpool(uuid).get
    try { Thread.sleep(10) } catch { case ex : InterruptedException => Thread.currentThread().interrupt() }
    val spool1a = rack updateSpool (uuid, ServiceSpool(spool0.spool, qinf1))
    val spool1b = rack.getSpool(uuid).get
    spool1a.spool should be(spool1b.spool)
    spool1a.tQueryInfo should be(spool1b.tQueryInfo)
    qinf1.expires.get should be < spool1a.tQueryInfo.expires.get
  }

  "TimedSpoolRack" should "do CRUD on spools" in {
    val rack = new TimedSpoolRack(planAndExecute = mockplanner)
    rack.createSpool(query, tquery.queryInfo)
    val spool = rack.getSpool(uuid)
    spool.nonEmpty should be(true)
    spool.get.spool shouldBe a[Spool[_]]
    rack.removeSpool(uuid)
    rack.getSpool(uuid).isEmpty should be(true)
  }

  it should "remove spool entries after ttl" in {
    val rack = new TimedSpoolRack(ttl = Duration.fromMilliseconds(10), planAndExecute = mockplanner)
    val timer = new JavaTimer(true)
    rack.createSpool(query, tquery.queryInfo)
    timer.doLater(Duration.fromMilliseconds(20))(rack.getSpool(uuid).isEmpty should be(true))
  }

  it should "alter ttl with updates" in {
    val rack = new TimedSpoolRack(planAndExecute = mockplanner)
    val qinf1 = rack createSpool (query, tquery.queryInfo)
    val spool0 = rack.getSpool(uuid).get
    try { Thread.sleep(10) } catch { case ex : InterruptedException => Thread.currentThread().interrupt() }
    val spool1a = rack updateSpool (uuid, ServiceSpool(spool0.spool, qinf1))
    val spool1b = rack.getSpool(uuid).get
    spool1a.spool should be(spool1b.spool)
    spool1a.tQueryInfo should be(spool1b.tQueryInfo)
    qinf1.expires.get should be < spool1a.tQueryInfo.expires.get
  }

  "SpoolPager" should "slice spools to consecutive pages" in {
    val rack = new TimedSpoolRack(planAndExecute = mockplanner)
    val qinf1 = rack createSpool (query, tquery.queryInfo)
    val spool0 = rack.getSpool(uuid).get
    val spoolsize0 = spool0.spool.toSeq.get.size
    val pair1 = new SpoolPager(spool0).page.get
    pair1._1.size should be(PGSZ)
    pair1._2.toSeq.get.size should be(spoolsize0 - PGSZ)
    val spool1 = rack updateSpool (uuid, ServiceSpool(pair1._2, tquery.queryInfo))
    val spoolsize1 = spool1.spool.toSeq.get.size
    val pair2 = new SpoolPager(spool1).page.get
    pair2._1.size should be(PGSZ)
    pair2._2.toSeq.get.size should be(spoolsize1 - PGSZ)
  }

  it should "convert row spools to page spools" in {
    val rack = new TimedSpoolRack(planAndExecute = mockplanner)
    val qinf1 = rack createSpool (query, tquery.queryInfo)
    val spool0 = rack.getSpool(uuid).get
    val spoolsize0 = spool0.spool.toSeq.get.size
    val pspool = new SpoolPager(spool0).pageAll.get
    pspool.toSeq.get.size should be((spoolsize0 / PGSZ) + 1)
  }

  "Chill-based page serialization" should "serialize page value objects" in {
    // prepare row page
    val rack = new TimedSpoolRack(planAndExecute = mockplanner)
    val qinf1 = rack createSpool (query, tquery.queryInfo)
    val spool0 = rack.getSpool(uuid).get
    val spoolsize0 = spool0.spool.toSeq.get.size
    val page1 = PageValue(new SpoolPager(spool0).pageAll.get.head, spool0.tQueryInfo)
    // bijection-based kryo serialization plus string encoding
    val pooledKryoInjection = KryoInjection.instance(KryoPoolSerialization.chill)
    val bytes = pooledKryoInjection(page1)
    // ... and back
    val page2 = pooledKryoInjection.invert(bytes).get
    page2 should be(page1)
  }

  it should "serialize page key objects" in {
    val key1 = PageKey(UUID.randomUUID(), 0)
    val pooledKryoInjection = KryoInjection.instance(KryoPoolSerialization.chill)
    val bytes = pooledKryoInjection(key1)
    val key2 = pooledKryoInjection.invert(bytes).get
    key2 should be(key1)
  }

  "RowConverter" should "transform query model rows to service model rows" in {
    val rack = new TimedSpoolRack(planAndExecute = mockplanner)
    val qinf1 = rack createSpool (query, tquery.queryInfo)
    val spool0 = rack.getSpool(uuid).get
    // SpoolPager.page calls row converter and returns a converted page
    val spage = new SpoolPager(spool0).page.get._1
    // page elements should be service model objects
    spage.head.isInstanceOf[ScrayTRow] should be(true)
    // column values should be properly serialized as ByteBuffers
    // look up original column value from native spool
    val col1 = spool0.spool.head.getColumnValue[Any](0)

    // decode corresponding column value from page via combined inverted injection/bijection
    val rawBytes = Bijection.bytes2Buffer.invert(spage.head.columns.get.head.value)
    // need to conditionally uncompress
    val col2 = KryoInjection.instance(KryoPoolSerialization.chill).invert({
      if (rawBytes.length >= PredefinedProperties.RESULT_COMPRESSION_MIN_SIZE.getDefault()) {
        RowConverter.bytes2SnappyBytes.invert(rawBytes)
      } else if (Snappy.isValidCompressedBuffer(rawBytes)) {
        RowConverter.bytes2SnappyBytes.invert(rawBytes)
      } else rawBytes
    })

    // w/o compression
    // val col2 = KryoInjection.instance(KryoPoolSerialization.chill).invert(
    // Bijection.bytes2Buffer.invert(spage.head.columns.get.head.value))

    col2.get should be(col1.get)
  }

}
