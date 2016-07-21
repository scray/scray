package scray.querying.source.costs

import scray.querying.queries.DomainQuery
import scray.querying.source.Source
import scray.querying.source.SimpleHashJoinSource
import scray.querying.source.KeyValueSource

object LinearQueryCostFuntionFactory {

  implicit object defaultFactory extends QueryCostFunctionFactory {
    override def getCostFunction[Q <: DomainQuery, T](source: Source[Q, T]): CostFunction[Q] = source match {
      case hashJoinSource: SimpleHashJoinSource[a, b, c] => new LinearSimpleHashJoinCosts(hashJoinSource, this)
      //case keyValueSource: KeyValueSource[k,v] => new KeyValueSourceCosts(keyValueSource, this)
    }
  }
}