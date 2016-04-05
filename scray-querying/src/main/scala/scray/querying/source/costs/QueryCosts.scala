package scray.querying.source.costs

/**
 * result of the getCosts operation
 */
case class QueryCosts(estimatedAmount: Double, estimatedCardinality: Long)