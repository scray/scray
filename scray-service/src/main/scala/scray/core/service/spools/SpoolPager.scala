package scray.core.service.spools

import scray.core.service._
import com.twitter.util.Future
import scray.service.qservice.thrifscala.ScrayTResultFrame

trait SpoolPager {
  def next(spool : ServiceSpool) : Future[ScrayTResultFrame]
}
