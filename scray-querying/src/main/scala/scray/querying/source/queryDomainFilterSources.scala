// See the LICENCE.txt file distributed with this work for additional
// information regarding copyright ownership.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.
package scray.querying.source

import scray.querying.description.Column
import scray.querying.description.EmptyRow
import scray.querying.description.Row
import scray.querying.description.internal.Domain
import scray.querying.description.internal.RangeValueDomain
import scray.querying.description.internal.SingleValueDomain
import scray.querying.queries.DomainQuery
import scray.querying.description.internal.ComposedMultivalueDomain
import scray.querying.description.internal.StringDomainConverter
import scray.querying.description.internal.BooleanDomainConverter
import scray.querying.description.internal.IntDomainConverter
import scray.querying.description.internal.LongDomainConverter
import scray.querying.description.internal.BigIntDomainConverter
import scray.querying.description.internal.JBigIntegerDomainConverter
import java.math.{BigInteger => JBigInteger, BigDecimal => JBigDecimal}
import scray.querying.description.internal.DoubleDomainConverter
import scray.querying.description.internal.BigDecimalDomainConverter
import scray.querying.description.internal.JBigDecimalDomainConverter
import scray.querying.description.internal.DomainTypeConverter
import com.twitter.util.Try

/**
 * Common code for domain checking
 */
object DomainFilterSource {
  
  /**
   * check if the provided value is compatible with the domains
   */
  def domainCheck[T](value: T, domain: Domain[_], 
      converter: Option[DomainTypeConverter[_]]): Boolean = domain match {
    case single: SingleValueDomain[T] => Try {
      !single.equiv.equiv(value, single.value)
    }.getOrElse(converter.map{converter =>
      val mapped = converter.mapDomain(domain).asInstanceOf[Option[SingleValueDomain[T]]]
      mapped.map(svd => !svd.equiv.equiv(value, svd.value)).getOrElse(true)}.getOrElse(true))
    case range: RangeValueDomain[T] => Try {
      !range.valueIsInBounds(value)
    }.getOrElse(converter.map{converter =>
      val mapped = converter.mapDomain(domain).asInstanceOf[Option[RangeValueDomain[T]]]
      mapped.map(rvd => Try(!rvd.valueIsInBounds(value)).getOrElse(true)).getOrElse(true)}.getOrElse(true))
    case composed: ComposedMultivalueDomain[T] => composed.domains.find(!domainCheck(value, _, converter)).isEmpty
  }
  
  /**
   * determine filter converter mappers for the value
   */
  @inline def getDomainConverter(value: Any): Option[DomainTypeConverter[_]] = value match {
    case s: String => Some(StringDomainConverter)
    case b: Boolean => Some(BooleanDomainConverter)
    case i: Int => Some(IntDomainConverter)
    case l: Long => Some(LongDomainConverter)
    case l: Double => Some(DoubleDomainConverter)
    case bi: BigInt => Some(BigIntDomainConverter)
    case bji: JBigInteger => Some(JBigIntegerDomainConverter)
    case db: BigDecimal => Some(BigDecimalDomainConverter)
    case dbj: JBigDecimal => Some(JBigDecimalDomainConverter)
    case _ => None
  } 
}

/**
 * used to filter rows according to the domain parameters supplied
 * TODO: exclude filters which have already been applied due to usage in database system 
 */
class LazyQueryDomainFilterSource[Q <: DomainQuery](source: LazySource[Q]) 
  extends LazyQueryMappingSource[Q](source) {
  
  override def transformSpoolElement(element: Row, query: Q): Row = {
    // if we find a domain which is not matched by this Row we throw it (the Row) away
    query.getWhereAST.find { domain =>
      element.getColumnValue[Any](domain.column) match {
        case None => true
        case Some(value) => DomainFilterSource.domainCheck(value, domain, DomainFilterSource.getDomainConverter(value))
      }
    } match {
      case None => element
      case Some(x) => new EmptyRow
    }
  }
  
  /**
   * LazyQueryDomainFilterSource doesn't throw away columns (only rows), 
   * so we report back all columns from upstream
   */
  override def getColumns: List[Column] = source.getColumns
  
  override def getDiscriminant = "Filter" + source.getDiscriminant
}


/**
 * used to filter rows according to the domain parameters supplied
 */
class EagerCollectingDomainFilterSource[Q <: DomainQuery, R](source: Source[Q, R]) 
  extends EagerCollectingQueryMappingSource[Q, R](source) {

  override def transformSeq(element: Seq[Row], query: Q): Seq[Row] = {
    element.filter { row => 
      query.getWhereAST.find { domain =>
        row.getColumnValue[Any](domain.column) match {
          case None => true
          case Some(value) => DomainFilterSource.domainCheck(value, domain, DomainFilterSource.getDomainConverter(value))
        }
      } match {
        case None => true
        case Some(x) => false
      }
    }
  }

  override def transformSeqElement(element: Row, query: Q): Row = element

  override def getColumns: List[Column] = source.getColumns
  
  override def getDiscriminant = "Filter" + source.getDiscriminant
}
