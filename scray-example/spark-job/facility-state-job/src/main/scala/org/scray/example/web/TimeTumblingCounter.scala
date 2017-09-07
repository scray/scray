package de.s_node.analyser.web

import java.util.Calendar

import com.typesafe.scalalogging.slf4j.LazyLogging
import org.scray.example.data.TimeDecorator
import scray.example.input.db.fasta.model.Facility

class TimeTumblingCounter extends LazyLogging {

  var inactiveeState = 0L
  var activeState = 0L
  var lastElementDate = Calendar.getInstance()

  /**
   * Dates timestamp should be added in increasing order.
   */
  def add(fac: TimeDecorator[Facility]): Option[Float] = {

    val elementDate = Calendar.getInstance()
    elementDate.setTimeInMillis(fac.getRequestTime)

    if (elementDate.compareTo(lastElementDate) > 0) {
      // Reset values if new day began
      if (!isSameDay(elementDate, lastElementDate)) {
        var inactiveeState = 0.0
        var activeState = 0.0
        var lastElementDate = Calendar.getInstance()
      }

      if (fac.getData.getState.toString() == "ACTIVE") {
        activeState += 1
        Some(activeState / inactiveeState)
      } else if (fac.getData.getState.toString() == "INACTIVE") {
        inactiveeState += 1
        Some(activeState / inactiveeState)
      } else {
        logger.warn(s"Unknown facility state for TimeTumblingCounter ${fac.getData.getState.toString()}")
        None
      }
    } else {
      logger.error("Element to add is older than last elmenet added")
      None
    }
  }

  def isSameDay(date1: Calendar, date2: Calendar): Boolean = synchronized {

    val isSameDay = date1.get(Calendar.DAY_OF_MONTH).equals(date2.get(Calendar.DAY_OF_MONTH)) &&
      date1.get(Calendar.MONTH).equals(date2.get(Calendar.MONTH)) &&
      date1.get(Calendar.YEAR).equals(date2.get(Calendar.YEAR))

    isSameDay
  }
}