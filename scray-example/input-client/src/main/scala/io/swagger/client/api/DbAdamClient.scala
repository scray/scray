package io.swagger.client.api

import java.util.Queue

import io.swagger.client.model.Facility
import com.typesafe.scalalogging.slf4j.LazyLogging

/**
 * Pull facility informations and store them in a given queue.
 */
class DbAdamClient(val outputQueue: Queue[Facility], samplingRate: Int) extends Thread with LazyLogging {

  override def run {
    val api = new DefaultApi
    
    while (!Thread.currentThread().isInterrupted()) {
      api.findFacilities.map(facilityInfos => {
        logger.info(s"Received ${facilityInfos.size} facility informations")
        facilityInfos.map( facility => outputQueue.add(facility))
      })
      Thread.sleep(samplingRate)
    }
  }
}
