package scray.querying.description.internal

import scray.querying.description.TableIdentifier
import scray.querying.description.Column
import scray.querying.description.TableConfiguration

/**
 * represents information to find a materialized view
 */
case class MaterializedView(
    fixedDomains: Array[(Column, Array[SingleValueDomain[_]])], // single value domains -> multiple possible values 
    rangeDomains: Array[(Column, Array[RangeValueDomain[_]])], // range value domains -> 
    viewTable: TableConfiguration[_, _, _] // table implementing this materialized view
)

