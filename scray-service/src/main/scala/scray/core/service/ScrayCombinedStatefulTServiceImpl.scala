// See the LICENCE.txt file distributed with this work for additional
// information regarding copyright ownership.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package scray.core.service

import java.nio.ByteBuffer
import java.util.UUID
import scala.util.Failure
import scala.util.Success
import org.parboiled2.ParseError
import com.twitter.util.Future
import scray.core.service.parser.TQueryParser
import scray.core.service.spools.SpoolRack
import scray.service.qmodel.thrifscala.ScrayUUID
import scray.service.qmodel.thrifscala.ScrayTQuery
import scray.service.qmodel.thrifscala.ScrayTQueryInfo
import scray.service.qmodel.thrifscala.ScrayTRow
import scray.service.qservice.thrifscala.ScrayTResultFrame
import scray.core.service.spools.SpoolPager
import scray.service.qservice.thrifscala.{ ScrayTServiceEndpoint, ScrayCombinedStatefulTService }
import scray.core.service.spools.ServiceSpool
import com.twitter.concurrent.Spool
import scray.querying.description.Row
import org.slf4j.LoggerFactory
import scray.core.service.spools.TSpoolRack
import com.typesafe.scalalogging.slf4j.LazyLogging

object ScrayCombinedStatefulTServiceImpl {
  def apply() = new ScrayCombinedStatefulTServiceImpl(TSpoolRack)
}

/**
 * A composition of StatefulTServiceImpl and ScrayMetaTServiceImpl
 */
class ScrayCombinedStatefulTServiceImpl(val rack: SpoolRack)
  extends ScrayCombinedStatefulTService.FutureIface with LazyLogging {

  /*
   * Stateful Query Service Operations
   */

  val statefulTServiceImpl = ScrayStatefulTServiceImpl()
  def query(tQuery: ScrayTQuery): Future[ScrayUUID] = statefulTServiceImpl.query(tQuery)
  def getResults(queryId: ScrayUUID): Future[ScrayTResultFrame] = statefulTServiceImpl.getResults(queryId)

  /*
   * Meta Service Operations
   */

  /**
   * Fetch a list of service endpoints.
   * Each endpoint provides ScrayStatelessTService and ScrayStatefulTService alternatives.
   * Queries can address different endpoints for load distribution.
   */
  def getServiceEndpoints(): Future[Seq[ScrayTServiceEndpoint]] =
    ScrayMetaTServiceImpl.getServiceEndpoints()

  /**
   * Add new service endpoint.
   * The endpoint will be removed after a default expiration period.
   */
  def addServiceEndpoint(tEndpoint: ScrayTServiceEndpoint): Future[ScrayTServiceEndpoint] =
    ScrayMetaTServiceImpl.addServiceEndpoint(tEndpoint)

  /**
   * Restore the default expiration period of an endpoint.
   */
  def refreshServiceEndpoint(endpointID: ScrayUUID): Future[Unit] =
    ScrayMetaTServiceImpl.refreshServiceEndpoint(endpointID)

  /**
   * Return vital sign.
   */
  def ping(): Future[Boolean] = ScrayMetaTServiceImpl.ping()

  /**
   * Shutdown the server.
   */
  def shutdown(waitNanos: Option[Long]): Future[Unit] = ScrayMetaTServiceImpl.shutdown(waitNanos)

}
