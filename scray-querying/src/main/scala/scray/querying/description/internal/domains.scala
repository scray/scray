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
package scray.querying.description.internal

import scala.math.Ordering
import scray.querying.description.Column


/**
 * represents a domain as a replacement for a predicate
 */
abstract class Domain[T](val column: Column)

/**
 * single value domain, represents effectively =
 */
case class SingleValueDomain[T](
  override val column: Column,
  value: T
)(implicit val equiv: Equiv[T]) extends Domain[T](column)

/**
 * A single interval on the values of column, represents <, >, <= or >=
 * This of course assumes that data can be ordered by some means.
 */
case class RangeValueDomain[T](
  override val column: Column,
  lowerBound: Option[Bound[T]], // None means no lower bound
  upperBound: Option[Bound[T]] // None means no upper bound
)(implicit val ordering: Ordering[T]) extends Domain[T](column) {
  /**
   * finds the smallest possible interval from two intervals
   */
  def bisect(rangeValueDomain: RangeValueDomain[T]): Option[RangeValueDomain[T]] = {
    val lower = if(rangeValueDomain.lowerBound.isEmpty) lowerBound else {
      rangeValueDomain.lowerBound.flatMap(llower => { lowerBound.map { _ =>
        val comp = ordering.compare(llower.value, lowerBound.get.value) 
        if(comp == 0) {
          Bound[T](llower.inclusive && rangeValueDomain.lowerBound.get.inclusive, llower.value)
        } else if(comp < 0) {
          lowerBound.get
        } else {
          llower
        }
      }.orElse(rangeValueDomain.lowerBound)
      })
    }
    val upper = if(rangeValueDomain.upperBound.isEmpty) upperBound else {
      rangeValueDomain.upperBound.flatMap(lupper => { upperBound.map { _ =>
        val comp = ordering.compare(lupper.value, upperBound.get.value)
        if(comp == 0) {
          Bound[T](lupper.inclusive && rangeValueDomain.upperBound.get.inclusive, lupper.value)
        } else if(comp > 0) {
          upperBound.get
        } else {
          lupper
        }
      }.orElse(rangeValueDomain.upperBound)
      })
    }
    lower match { 
      case None => if(upper.isDefined && column == rangeValueDomain.column) {
          Some(RangeValueDomain[T](column, None, upper)) 
        } else { 
          None 
        } 
      case Some(newLower) => upper match {
          case None => Some(RangeValueDomain[T](column, lower, None))
          case Some(newUpper) => if((column != rangeValueDomain.column) ||
            (ordering.compare(newLower.value, newUpper.value) > 0) || 
            ((!(newLower.inclusive && newUpper.inclusive)) && (ordering.compare(newLower.value, newUpper.value) == 0))) {
               None
          } else { Some(RangeValueDomain[T](column, lower, upper)) }
      }
    }
  }
  def valueIsInBounds(value: T): Boolean = {
    lowerBound.map(lower => ordering.compare(lower.value, value) < 0 || 
        (lower.inclusive && ordering.compare(lower.value, value) == 0)).getOrElse(true) &&
    upperBound.map(upper => ordering.compare(upper.value, value) > 0 || 
        (upper.inclusive && ordering.compare(upper.value, value) == 0)).getOrElse(true)
  }
  def valueIsHigherThanUpper(value: T): Boolean = {
    upperBound.map(upper => ordering.compare(value, upper.value) > 0 ||
        (upper.inclusive && ordering.compare(upper.value, value) == 0)).getOrElse(false)
  }
  def valueIsLowerThanLower(value: T): Boolean = {
    lowerBound.map(lower => ordering.compare(value, lower.value) < 0 ||
        (lower.inclusive && ordering.compare(lower.value, value) == 0)).getOrElse(false)
  }
}

/**
 * represents a domain which has multiple ranges, i.e. is
 * composed of several other domains 
 */
case class ComposedMultivalueDomain[T](override val column: Column, domains: Set[Domain[T]]) 
  extends Domain[T](column)


/**
 * represents a bound on some interval
 */
case class Bound[T: Ordering](inclusive: Boolean, value: T)
