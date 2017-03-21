package scray.querying.description.internal

import scray.querying.description.TableIdentifier
import scray.querying.description.Column
import scray.querying.description.TableConfiguration
import scray.querying.queries.DomainQuery
import scray.common.key.api.KeyGenerator

/**
 * Contains information to find a materialized view
 * 
 * @param krimaryKey Primary key column in database
 */
case class MaterializedView(
    val table:TableIdentifier,
    val keyGenerationClass: KeyGenerator[_],
    val primaryKeyColumn: String = "key"
    //val fixedDomains: Array[Column]//, // single value domains -> multiple possible values 
    //rangeDomains: Array[(Column, Array[RangeValueDomain[_]])], // range value domains -> 
    //viewTable: TableConfiguration[_ <: DomainQuery, _ <: DomainQuery, _], // table implementing this materialized view
    //checkMaterializedView: (MaterializedView, DomainQuery) => Option[(Boolean, Int)]
) 

