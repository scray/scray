package scray.querying.description.internal

import scray.querying.description.TableIdentifier
import scray.querying.description.Column
import scray.querying.description.TableConfiguration
import scray.querying.queries.DomainQuery
import scray.common.key.api.KeyGenerator

/**
 * represents information to find a materialized view
 */
case class MaterializedView(
    val table:TableIdentifier,
    val keyGenerationClass: KeyGenerator[_]
    //val fixedDomains: Array[Column]//, // single value domains -> multiple possible values 
    //rangeDomains: Array[(Column, Array[RangeValueDomain[_]])], // range value domains -> 
    //viewTable: TableConfiguration[_ <: DomainQuery, _ <: DomainQuery, _], // table implementing this materialized view
    //checkMaterializedView: (MaterializedView, DomainQuery) => Option[(Boolean, Int)]
) 

