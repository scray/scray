package scray.core.service

import java.nio.ByteBuffer
import java.util.UUID
import scala.util.Failure
import scala.util.Success
import org.parboiled2.ParseError
import com.twitter.util.Future
import scray.core.service.parser.TQueryParser
import scray.core.service.spools.SpoolRack
import scray.service.base.thrifscala.ScrayUUID
import scray.service.qmodel.thrifscala.ScrayTQuery
import scray.service.qmodel.thrifscala.ScrayTQueryInfo
import scray.service.qmodel.thrifscala.ScrayTRow
import scray.service.qservice.thrifscala.ScrayTResultFrame
import scray.service.qservice.thrifscala.ScrayTService
import scray.core.service.spools.SpoolPager

class ScrayTServiceImpl(val rack : SpoolRack, val pager : SpoolPager) extends ScrayTService.FutureIface {

  def query(tQuery : ScrayTQuery) : Future[ScrayUUID] = {
    val parser = new TQueryParser(tQuery)
    val parsed = parser.InputLine.run() match {
      case Success(result) => Success(result)
      case Failure(e : ParseError) =>
        sys.error(parser.formatError(e, showTraces = true)); Failure(e)
      case Failure(e) => throw e
    }
    parsed.flatMap(_.createQuery) match {
      case Success(query) => rack.putSpool(query, tQuery.queryInfo).map { _.queryId.get }
      case Failure(ex) => Future.exception(ex)
    }
  }

  def getResults(queryId : ScrayUUID, offset : Int) : Future[ScrayTResultFrame] = {
    rack.getSpool(queryId).flatMap(spool => pager.next(spool))
  }

}
