package scray.core.service.spools

import org.slf4j.LoggerFactory
import com.twitter.concurrent.Spool
import com.twitter.finagle.memcached.Client
import com.twitter.storehaus.memcache.MemcacheStore
import com.twitter.util.Duration
import com.twitter.util.Future
import com.twitter.util.Time
import scray.core.service.MEMCACHED_HOST
import scray.core.service.UUID2ScrayUUID
import scray.querying.Query
import scray.querying.description.Row
import scray.service.qmodel.thrifscala.ScrayTQueryInfo
import scray.service.qmodel.thrifscala.ScrayUUID
import java.util.concurrent.ExecutorService
import java.util.concurrent.Executors
import scray.service.qmodel.thrifscala.ScrayTRow

class MemcachedPageRack(
  planAndExecute : (Query) => Spool[Row],
  underlying : MemcacheStore)
  extends PageRack {

  private val logger = LoggerFactory.getLogger(classOf[MemcachedPageRack])

  val client = Client(MEMCACHED_HOST)
  val store = MemcacheStore(client, pageTTL, 0)

  // computes expiration time for collecting frames
  private def expires = Time.now + pageTTL

  // monitoring wrapper for planner function
  private val wrappedPlanAndExecute : (Query) => Spool[Row] = (q) => { planLog(q); planAndExecute(q) }
  private def planLog(q : Query) : Unit = logger.info(s"Planner called for query $q");

  val POOLSIZE = 10
  val pool : ExecutorService = Executors.newFixedThreadPool(POOLSIZE)

  def createPages(query : Query, tQueryInfo : ScrayTQueryInfo) : ScrayTQueryInfo = {

    // exit if exists
    val check = store.get(query.getQueryID.toString)

    // fix expiration duration
    val expiration = expires

    // pull the query id
    val quid = query.getQueryID

    // pull the page size
    val pagesize : Int = tQueryInfo.pagesize.getOrElse(DEFAULT_PAGESIZE)

    //update query info
    val updQI = tQueryInfo.copy(
      queryId = Some(quid),
      expires = Some(expiration.inNanoseconds))

    // prepare this query with the engine
    val resultSpool : Spool[Row] = wrappedPlanAndExecute(query)

    // spawn paging job
    pool.execute(new MemcachedSpoolPager(ServiceSpool(resultSpool, updQI)))

    // return updated query info
    updQI
  }

  def setPage(id : PageKey, page : PageValue, ttl : Duration = pageTTL) : Unit = ???
  def getPage(key : PageKey) : Future[Option[PageValue]] = ???
  def removePages(uuid : ScrayUUID) : Unit = ???
  def removePage(key : PageKey) : Unit = ???

}

class MemcachedSpoolPager(serviceSpool : ServiceSpool) extends Runnable {

  def run() {
    val pages : Future[Spool[Seq[Row]]] = (new SpoolPager(serviceSpool)).pageAll()
    // put pages to memcached
  }

}
