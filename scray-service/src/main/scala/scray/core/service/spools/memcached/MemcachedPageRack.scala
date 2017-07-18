package scray.core.service.spools.memcached

import java.util.concurrent.ExecutorService
import java.util.concurrent.Executors
import java.net.InetSocketAddress
import org.slf4j.LoggerFactory
import scala.annotation.tailrec
import scala.collection.JavaConversions._
import com.twitter.concurrent.Spool
import com.twitter.finagle.memcachedx.Client
import com.twitter.util.Duration
import com.twitter.util.Future
import com.twitter.util.Time
import scray.core.service._
import scray.core.service.ScrayUUID2UUID
import scray.core.service.UUID2ScrayUUID
import scray.core.service.spools._
import scray.querying.Query
import scray.querying.description.Row
import scray.service.qmodel.thrifscala.ScrayTQueryInfo
import scray.service.qmodel.thrifscala.ScrayUUID
import com.typesafe.scalalogging.LazyLogging

class MemcachedPageRack(planAndExecute: (Query) => Spool[Row], pageTTL: Duration = DEFAULT_TTL)
  extends PageRackImplBase(planAndExecute, pageTTL) {

  val clientOption = tryClient(MEMCACHED_ENDPOINTS)

  @tailrec
  final def tryClient(meps: scala.collection.mutable.Set[InetSocketAddress]): Option[Client] = {
    if (meps.isEmpty) return None
    else {
      val isa = meps.iterator.next
      try {
        return Some(Client(host = inetAddr2EndpointString(isa)))
      } catch {
        case e: Exception => return {
          logger.warn(s"Couldn't bind to memcached socket address ${isa}.", e)
          tryClient(meps - isa)
        }
      }
    }
  }

  val pageStore = MemcachePageStore(clientOption.get, pageTTL)

  val POOLSIZE = 10
  val pool: ExecutorService = Executors.newFixedThreadPool(POOLSIZE)

  override def createPages(query: Query, tQueryInfo: ScrayTQueryInfo): ScrayTQueryInfo = {
    // exit if exists (first page)
    if (pageStore.get(pidKeyEncoder(PageKey(query.getQueryID, 0))).get.isDefined) return tQueryInfo

    //update query info
    val updQI = tQueryInfo.copy(
      queryId = Some(query.getQueryID),
      expires = Some(expires.inNanoseconds))

    val snap1 = System.currentTimeMillis()

    // prepare this query with the engine
    val resultSpool: Spool[Row] = wrappedPlanAndExecute(query)

    logger.info(s"PlanAndExecute of query ${updQI.queryId.get} finished in ${System.currentTimeMillis() - snap1} milis.")
    val snap2 = System.currentTimeMillis()

    // prepare paging (lazily)
    val pages: Spool[Seq[Row]] = (new SpoolPager(ServiceSpool(resultSpool, updQI))).pageAll().get

    logger.info(s"Paging of query ${updQI.queryId.get} finished in ${System.currentTimeMillis() - snap2} milis.")
    val snap3 = System.currentTimeMillis()

    // push first page to memcached (in order to have it ready for subsequent client calls)
    pageStore.put(pidKeyEncoder(PageKey(updQI.queryId.get, 0)) -> Some(PageValue(pages.head, updQI)))

    logger.info(s"First page of query ${updQI.queryId.get} pushed to memcached finished in ${System.currentTimeMillis() - snap3} milis.")

    // spawn subsequent paging job
    pool.execute(new MemcachedSpoolPager(pages.tail, updQI, pageStore))

    // return updated query info
    updQI
  }

  override def getPage(key: PageKey): Future[Option[PageValue]] = pageStore.get(pidKeyEncoder(key))
}

class MemcachedSpoolPager(pages: Future[Spool[Seq[Row]]], queryInfo: ScrayTQueryInfo, store: MemcachePageStore) extends Runnable {
  private val logger = LoggerFactory.getLogger(classOf[MemcachedSpoolPager])

  def run() {
    val snap = System.currentTimeMillis()

    // alternative1: put pages to memcached in a single batch
    //    store.multiPut(pages.get.foldLeft(
    //      (Map[String, Option[PageValue]](), 1 /* indexing starts with second page (index=1)*/)) {
    //        (a, b) =>
    //          (a._1 + (pidKeyEncoder(PageKey(serviceSpool.tQueryInfo.queryId.get, a._2)) ->
    //            Some(PageValue(b, serviceSpool.tQueryInfo))), a._2 + 1)
    //      }.get._1)

    // alternative2: put pages to memcached sequentially (adds multiple network roundtrips but first page will be available fast)
    pushPages(pages.get, 1) // indexing starts with second page (index=1)
    logger.info(s"Putting pages of query ${queryInfo.queryId.get} to memcached finished in ${System.currentTimeMillis() - snap} milis.")
  }

  @tailrec
  private final def pushPages(pages: Spool[Seq[Row]], pageIdx: Int): Unit = if (!pages.isEmpty) {
    // val snap = System.currentTimeMillis()
    store.put(pidKeyEncoder(PageKey(queryInfo.queryId.get, pageIdx)) -> Some(PageValue(pages.head, queryInfo)))
    // logger.info(s"Putting 1 page to memcached finished in ${System.currentTimeMillis() - snap} milis.")
    pushPages(pages.tail.get, pageIdx + 1)
  }

}
