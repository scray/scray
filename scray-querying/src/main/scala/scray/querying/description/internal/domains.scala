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
sealed abstract class Domain[T](val column: Column)

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
  /**
   * makes a union of two RangeValueDomain[T]; if possible return Some(newDomain), return None otherwise
   */
  def union(rangeValueDomain: RangeValueDomain[T]): Option[RangeValueDomain[T]] = rangeValueDomain.lowerBound match {
    case None => lowerBound match {
      case None => rangeValueDomain.upperBound match {
        case None => Some(rangeValueDomain)
        case Some(upper) =>
          upperBound match {
            case None => Some(new RangeValueDomain[T](column, None, None))
            case Some(thisupper) => 
              val ord = ordering.compare(thisupper.value, upper.value)
              val res = if(ord >= 0) { 
                (thisupper.value, if(ord > 0) { thisupper.inclusive } else { thisupper.inclusive | upper.inclusive })
              } else {
                (upper.value, upper.inclusive)
              }
              Some(new RangeValueDomain[T](column, None, Some(new Bound[T](res._2, res._1))))
          }
      }
      case Some(thislower) => rangeValueDomain.upperBound match {
        case None => Some(rangeValueDomain)
        case Some(upper) =>
          upperBound match {
            case None =>
              val ord = ordering.compare(thislower.value, upper.value)
              if(ord < 0 || (ord == 0 && (thislower.inclusive | upper.inclusive))) {  
                Some(new RangeValueDomain[T](column, None, None))
              } else {
                None
              } 
            case Some(thisupper) =>
              val ord = ordering.compare(thisupper.value, upper.value)
              if(ord < 0) {
                Some(new RangeValueDomain[T](column, None, Some(upper)))
              } else {
                if(ord == 0) {
                  Some(new RangeValueDomain[T](column, None, 
                      Some(new Bound[T](thisupper.inclusive | upper.inclusive, upper.value))))
                } else {
                  val low = ordering.compare(thislower.value, upper.value)
                  if(ord < 0 || (ord == 0 && (thislower.inclusive | upper.inclusive))) {
                    Some(new RangeValueDomain[T](column, None, Some(thisupper)))
                  } else {
                    None
                  }
                }
              }
          }
      }
    }
    case Some(lower) => lowerBound match {
      case None => rangeValueDomain.upperBound match {
        case None => upperBound match {
          case None => Some(this)
          case Some(thisupper) =>
            val ord = ordering.compare(lower.value, thisupper.value)
            if(ord < 0 || ord == 0 && (lower.inclusive | thisupper.inclusive)) {
              Some(new RangeValueDomain[T](column, None, None))
            } else {
              None
            }
        }
        case Some(upper) => upperBound match {
          case None => Some(this)
          case Some(thisupper) =>
            val ord = ordering.compare(lower.value, thisupper.value)
            if(ord < 0 || ord == 0 && (lower.inclusive | thisupper.inclusive)) {
              val up = ordering.compare(upper.value, thisupper.value)
              if(up < 0) {
                Some(new RangeValueDomain[T](column, None, Some(thisupper)))
              } else {
                if(up == 0) {
                  Some(new RangeValueDomain[T](column, None, 
                      Some(new Bound[T](thisupper.inclusive | upper.inclusive, upper.value))))
                } else {
                  Some(new RangeValueDomain[T](column, None, Some(upper)))
                }
              }
            } else {
              None
            }
        }
      }
      case Some(thislower) => rangeValueDomain.upperBound match {
        case None => upperBound match {
          case None =>
            val ord = ordering.compare(lower.value, thislower.value)
            if(ord < 0) {
              Some(new RangeValueDomain[T](column, Some(lower), None))
            } else {
              if(ord == 0) {
                Some(new RangeValueDomain[T](column, 
                    Some(new Bound[T](thislower.inclusive | lower.inclusive, lower.value)), None))
              } else {
                Some(new RangeValueDomain[T](column, Some(thislower), None))
              }
            }
          case Some(thisupper) => 
            val ord = ordering.compare(lower.value, thisupper.value)
            if(ord > 0 || ord == 0 && !(lower.inclusive | thisupper.inclusive)) {
              None
            } else {
              ordering.compare(lower.value, thislower.value) match {
                case a if a > 0 => Some(new RangeValueDomain[T](column, Some(thislower), None))
                case b if b == 0 => Some(new RangeValueDomain[T](column, 
                    Some(new Bound[T](thislower.inclusive | lower.inclusive, lower.value)), None))
                case _ => Some(new RangeValueDomain[T](column, Some(lower), None))
              }
            }
        }
        case Some(upper) => upperBound match {
          case None =>
            val ord = ordering.compare(thislower.value, upper.value)
            if(ord > 0 || ord == 0 && !(thislower.inclusive | upper.inclusive)) {
              None
            } else {
              ordering.compare(thislower.value, lower.value) match {
                case a if a > 0 => Some(new RangeValueDomain[T](column, Some(lower), None))
                case b if b == 0 => Some(new RangeValueDomain[T](column, 
                    Some(new Bound[T](thislower.inclusive | lower.inclusive, lower.value)), None))
                case _ => Some(new RangeValueDomain[T](column, Some(thislower), None))
              }
            }
          case Some(thisupper) =>
            ordering.compare(thisupper.value, lower.value) match {
              case a if a > 0 => ordering.compare(thislower.value, upper.value) match {
                case a1 if a1 < 0 => ordering.compare(thisupper.value, upper.value) match {
                  case a11 if a11 >= 0 =>
                    if(ordering.compare(lower.value, thislower.value) <= 0) {
                      Some(new RangeValueDomain[T](column,
                          Some(new Bound[T](thislower.inclusive | lower.inclusive, lower.value)),
                          Some(new Bound[T](thislower.inclusive | lower.inclusive, thisupper.value))))
                    } else {
                      Some(new RangeValueDomain[T](column,
                          Some(new Bound[T](thislower.inclusive | lower.inclusive, thislower.value)),
                          Some(new Bound[T](thislower.inclusive | lower.inclusive, thisupper.value))))                      
                    }
                  case a12 if a12 < 0 =>
                    Some(rangeValueDomain)
                }
                case a2 if a2 == 0 && (upper.inclusive | thislower.inclusive) =>
                  Some(new RangeValueDomain[T](column, Some(lower), Some(thisupper)))
                case _ => None
              }
              case b if b == 0 && (thisupper.inclusive | lower.inclusive) => 
                Some(new RangeValueDomain[T](column, Some(thislower), Some(upper)))
              case _ => None
            }
        }
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
 * composed of several other domains which are combined by or
 */
case class ComposedMultivalueDomain[T](override val column: Column, domains: Set[Domain[T]]) 
  extends Domain[T](column)


/**
 * represents a bound on some interval
 */
case class Bound[T: Ordering](inclusive: Boolean, value: T)
