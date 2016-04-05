package scray.querying.queries

import scala.collection.mutable.MutableList

object CostEstimationInfo {
    val estimatedCosts: MutableList[Long => Long] = new MutableList[Long => Long]()
    
    def addNewCosts(costs: Long => Long) {
      estimatedCosts += costs
    }
    
    def getCosts(): Double = {
      println(estimatedCosts.size)
      estimatedCosts.reverse.foldLeft(0L)((b, a) => a.apply(b))
    }
  
}