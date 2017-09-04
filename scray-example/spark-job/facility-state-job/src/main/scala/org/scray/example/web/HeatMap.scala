package de.s_node.analyser.web

import scala.collection.mutable.MutableList

class HeatMap {
  case class HeatMapDayEntry(facId: String, timestamp: Long, availibility: Int)
  
  val days = new MutableList[HeatMapDayEntry]
  
  def addDay(facId: String, timestamp: Long, availibility: Int) = {
    days += (HeatMapDayEntry(facId, timestamp, availibility))
  }
  
  def aggregate[T](f: (T, HeatMapDayEntry) => T, outputFact: () => T): T = {
    days.foldLeft(outputFact())(f)
  }
}