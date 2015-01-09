package scray.core.service.spools.memcached

import java.util.concurrent.ExecutorService
import java.util.concurrent.Executors

import scala.annotation.tailrec

import org.slf4j.LoggerFactory

import com.twitter.concurrent.Spool
import com.twitter.finagle.memcached.Client
import com.twitter.util.Duration
import com.twitter.util.Future
import com.twitter.util.Time

import scray.core.service.MEMCACHED_HOST
import scray.core.service.ScrayUUID2UUID
import scray.core.service.UUID2ScrayUUID
import scray.core.service.spools._
import scray.querying.Query
import scray.querying.description.Row
import scray.service.qmodel.thrifscala.ScrayTQueryInfo
import scray.service.qmodel.thrifscala.ScrayUUID

class MemcachedPageRack(planAndExecute : (Query) => Spool[Row], val pageTTL : Duration = DEFAULT_TTL) extends PageRack {

  private val logger = LoggerFactory.getLogger(classOf[MemcachedPageRack])

  val client = Client(host = MEMCACHED_HOST)
  val pageStore = MemcachePageStore(client, pageTTL)

  // computes expiration time for collecting frames
  private def expires = Time.now + pageTTL

  // monitoring wrapper for planner function
  private val wrappedPlanAndExecute : (Query) => Spool[Row] = (q) => { planLog(q); planAndExecute(q) }
  private def planLog(q : Query) : Unit = logger.info(s"Planner called for query $q");

  val POOLSIZE = 10
  val pool : ExecutorService = Executors.newFixedThreadPool(POOLSIZE)

  override def createPages(query : Query, tQueryInfo : ScrayTQueryInfo) : ScrayTQueryInfo = {
    // exit if exists (first page)
    if (pageStore.get(pidKeyEncoder(PageKey(query.getQueryID, 0))).get.isDefined) return tQueryInfo

    //update query info
    val updQI = tQueryInfo.copy(
      queryId = Some(query.getQueryID),
      expires = Some(expires.inNanoseconds))

    // prepare this query with the engine
    val resultSpool : Spool[Row] = wrappedPlanAndExecute(query)

    // spawn paging job
    pool.execute(new MemcachedSpoolPager(ServiceSpool(resultSpool, updQI), pageStore))

    // return updated query info
    updQI
  }

  override def getPage(key : PageKey) : Future[Option[PageValue]] = pageStore.get(pidKeyEncoder(key))
}

class MemcachedSpoolPager(serviceSpool : ServiceSpool, store : MemcachePageStore) extends Runnable {
  private val logger = LoggerFactory.getLogger(classOf[MemcachedSpoolPager])

  def run() {
    // prepare paging (lazily)
    val pages : Future[Spool[Seq[Row]]] = (new SpoolPager(serviceSpool)).pageAll()

    val snap = System.currentTimeMillis()

    // put pages to memcached in a single batch
    store.multiPut(pages.get.foldLeft(
      (Map[String, Option[PageValue]](), 0)) {
        (a, b) =>
          (a._1 + (pidKeyEncoder(PageKey(serviceSpool.tQueryInfo.queryId.get, a._2)) ->
            Some(PageValue(b, serviceSpool.tQueryInfo))), a._2 + 1)
      }.get._1)

    logger.info(s"Putting pages to memcached finished in ${System.currentTimeMillis() - snap} milis.")

    // alternative: put pages to memcached sequentially (adds multiple network roundtrips but first page will be available fast)
    //pushPages(pages.get, 0)
    //logger.info(s"Putting pages to memcached finished in ${System.currentTimeMillis() - snap} milis.")
  }

  @tailrec
  private final def pushPages(pages : Spool[Seq[Row]], pageIdx : Int) : Unit = if (!pages.isEmpty) {
    val snap = System.currentTimeMillis()
    store.put(pidKeyEncoder(PageKey(serviceSpool.tQueryInfo.queryId.get, pageIdx)) -> Some(PageValue(pages.head, serviceSpool.tQueryInfo)))
    logger.info(s"Putting 1 page to memcached finished in ${System.currentTimeMillis() - snap} milis.")
    pushPages(pages.tail.get, pageIdx + 1)
  }
}
