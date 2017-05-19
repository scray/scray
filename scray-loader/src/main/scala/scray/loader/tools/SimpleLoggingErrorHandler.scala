package scray.loader.tools

import com.typesafe.scalalogging.slf4j.LazyLogging
import scray.common.errorhandling.ErrorHandler
import scray.common.errorhandling.LoadCycle
import scray.common.errorhandling.ScrayProcessableErrors

class SimpleLoggingErrorHandler extends ErrorHandler with LazyLogging {
  
  override def handleError(error: ScrayProcessableErrors, loadCycle: LoadCycle): Unit =
    handleError(error, loadCycle, None)
    
	override def handleError(error: ScrayProcessableErrors, loadCycle: LoadCycle, message: String): Unit =
	  handleError(error, loadCycle, Some(message))

	def handleError(error: ScrayProcessableErrors, loadCycle: LoadCycle, message: Option[String]): Unit = {
	  logger.info(s"${error.getIdentifier} in state ${loadCycle.toString} ${message.map { msg => s"Message: '${msg}'"}.getOrElse{""}}")
	}
}