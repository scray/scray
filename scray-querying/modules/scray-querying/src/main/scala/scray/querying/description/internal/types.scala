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

import java.math.{BigInteger => JBigInteger, BigDecimal => JBigDecimal}
import com.twitter.util.Try

/**
 * trait to convert types from queries and database result sets.
 * This is useful in the context of queries:
 * - transform domains such that they match the target types of 
 *   the database system (i.e. transformation of queries)
 * - transform domains such that they are compatible with
 *   the types used in the result sets
 */
sealed trait DomainTypeConverter[A] {

  /**
   * checks whether provided obj is compatible with the
   * current type converter 
   */
  def isConvertibleValue(obj: Any): Boolean
  
  /**
   * checks whether provided domain is compatible with the
   * current type converter 
   */
  def isConvertibleDomain(dom: Domain[_]): Boolean
  
  /**
   * converts a value into the the type of the target domain
   */
  def mapValue(value: Any): Option[A]
  
  /**
   * converts a value into the the type of the target domain and creates
   * the target domain
   */
  def mapDomain(orig: Domain[_]): Option[Domain[A]]
}

abstract class SingleValueDomainConverter[A](implicit equiv: Equiv[A]) extends DomainTypeConverter[A] {
  override def isConvertibleDomain(dom: Domain[_]): Boolean = dom match {
    case single: SingleValueDomain[_] => isConvertibleValue(single.value)
    case composed: ComposedMultivalueDomain[_] => composed.domains.filterNot(isConvertibleDomain(_)).isEmpty
    case range: RangeValueDomain[_] => false
  }

  override def mapDomain(orig: Domain[_]): Option[Domain[A]] = orig match {
    case single: SingleValueDomain[_] => mapValue(single.value).map(SingleValueDomain(orig.column, _)(equiv))
    case composed: ComposedMultivalueDomain[_] => isConvertibleDomain(composed) match {
      case true => Some(ComposedMultivalueDomain(composed.column, composed.domains.map { mapDomain(_).get }))
      case false => None
    }
    case range: RangeValueDomain[_] => None
  }
}

/**
 * domain converter for target domains of type String
 */
object StringDomainConverter extends SingleValueDomainConverter[String] {
  
  override def isConvertibleValue(obj: Any): Boolean = true

  override def mapValue(value: Any): Option[String] = value match {
    case bd: BigDecimal => Some(bd.bigDecimal.toPlainString)
    case jdb: JBigDecimal => Some(jdb.toPlainString)
    case str: String => Some(str)
    case a => Some(a.toString)
  }
}

/**
 * domain converter for boolean types
 */
object BooleanDomainConverter extends SingleValueDomainConverter[Boolean] {
  
  private val BigIntZero = BigInt(0)
  private val BigDecimalZero = BigDecimal("0")
  
  def isConvertibleValue(obj: Any): Boolean = obj match {
    case b: Byte => true
    case i: Int => true
    case d: Double => true
    case f: Float => true
    case s: Short => true
    case l: Long => true
    case bi: BigInt => true
    case bd: BigDecimal => true
    case jbi: JBigInteger => true
    case jdb: JBigDecimal => true
    case str: String if str.equalsIgnoreCase("true") || str.equalsIgnoreCase("false") => true
    case _ => false
  }

  override def mapValue(value: Any): Option[Boolean] = value match {
    case b: Byte => Some(b != 0)
    case i: Int => Some(i != 0)
    case d: Double => Some(d <= Double.MinPositiveValue && d >= 0D)
    case f: Float => Some(f <= Float.MinPositiveValue && f >= 0F)
    case s: Short => Some(s != 0)
    case l: Long => Some(l != 0L)
    case bi: BigInt => Some(bi.compare(BigIntZero) != 0)
    case bd: BigDecimal => Some(bd.compare(BigDecimalZero) != 0)
    case jbi: JBigInteger => Some(jbi.compareTo(JBigInteger.ZERO) != 0)
    case jdb: JBigDecimal => Some(jdb.compareTo(JBigDecimal.ZERO) != 0)
    case str: String => 
      if(str.equalsIgnoreCase("true")) {
        Some(true)
      } else {
        if(str.equalsIgnoreCase("false")) {
          Some(false)
        } else {
          None
        }
      } 
    case _ => None
  }
}


/**
 * generic class for numeric types
 */
abstract class NumberDomainConverter[A](pattern: String)(implicit ordering: Ordering[A], equiv: Equiv[A]) 
    extends DomainTypeConverter[A] {
  
  override def isConvertibleValue(obj: Any): Boolean = obj match {
    case b: Byte => true
    case i: Int => true
    case f: Float => true
    case s: Short => true
    case l: Long => true
    case d: Double => true
    case bi: BigInt => true
    case bd: BigDecimal => true
    case jbi: JBigInteger => true
    case jdb: JBigDecimal => true
    case str: String => str.matches(pattern)
    case _ => false
  }

  override def isConvertibleDomain(dom: Domain[_]): Boolean = dom match {
    case single: SingleValueDomain[_] => isConvertibleValue(single.value)
    case range: RangeValueDomain[_] => (range.lowerBound.isEmpty || isConvertibleValue(range.lowerBound.get.value)) &&
      (range.upperBound.isEmpty || isConvertibleValue(range.upperBound.get.value))
    case composed: ComposedMultivalueDomain[_] => composed.domains.filterNot(isConvertibleDomain(_)).isEmpty
  }
  
  override def mapDomain(orig: Domain[_]): Option[Domain[A]] = orig match {
    case single: SingleValueDomain[_] => mapValue(single.value).map(SingleValueDomain(orig.column, _)(equiv))
    case range: RangeValueDomain[_] => Try(RangeValueDomain(orig.column,
        range.lowerBound.flatMap(bound => mapValue(bound.value).map(Bound(bound.inclusive, _))),
        range.upperBound.flatMap(bound => mapValue(bound.value).map(Bound(bound.inclusive, _)))
        )(ordering)).toOption
    case composed: ComposedMultivalueDomain[_] => isConvertibleDomain(composed) match {
      case true => Some(ComposedMultivalueDomain(composed.column, composed.domains.map { mapDomain(_).get }))
      case false => None
    }
  }
}

/**
 * domain converter for target domains of type double
 */
object DoubleDomainConverter extends NumberDomainConverter[Double]("^[\\+\\-]?(\\d+\\.)?\\d+(E[\\+\\-]?\\d+)?$") {  
  override def mapValue(value: Any): Option[Double] = value match {
    case b: Byte => Some(Byte.byte2double(b))
    case i: Int => Some(Int.int2double(i))
    case f: Float => Some(Float.float2double(f))
    case s: Short => Some(Short.short2double(s))
    case d: Double => Some(d)
    case l: Long => Some(Long.long2double(l))
    case bi: BigInt => Some(bi.doubleValue())
    case bd: BigDecimal => Some(bd.doubleValue())
    case jbi: JBigInteger => Some(jbi.doubleValue())
    case jdb: JBigDecimal => Some(jdb.doubleValue())
    case str: String => Try(str.toDouble).toOption
    case _ => None
  }
}

/**
 * domain converter for target domains of type int
 */
object IntDomainConverter extends NumberDomainConverter[Int]("^[\\+\\-]?\\d+$") {
  override def mapValue(value: Any): Option[Int] = value match {
    case b: Byte => Some(Byte.byte2int(b))
    case i: Int => Some(i)
    case f: Float => Some(f.toInt)
    case d: Double => Some(d.toInt)
    case s: Short => Some(Short.short2int(s))
    case l: Long => Some(l.toInt)
    case bi: BigInt => Some(bi.intValue())
    case bd: BigDecimal => Some(bd.intValue())
    case jbi: JBigInteger => Some(jbi.intValue())
    case jdb: JBigDecimal => Some(jdb.intValue())
    case str: String => Try(str.toInt).toOption
    case _ => None
  }
}

/**
 * domain converter for target domains of type Long
 */
object LongDomainConverter extends NumberDomainConverter[Long]("^[\\+\\-]?\\d+$") {
  override def mapValue(value: Any): Option[Long] = value match {
    case b: Byte => Some(Byte.byte2long(b))
    case i: Int => Some(i.toLong)
    case f: Float => Some(f.toLong)
    case d: Double => Some(d.toLong)
    case s: Short => Some(Short.short2long(s))
    case l: Long => Some(l)
    case bi: BigInt => Some(bi.longValue())
    case bd: BigDecimal => Some(bd.longValue())
    case jbi: JBigInteger => Some(jbi.longValue())
    case jdb: JBigDecimal => Some(jdb.longValue())
    case str: String => Try(str.toLong).toOption
    case _ => None
  }
}

/**
 * domain converter for target domains of type BigInt
 */
object BigIntDomainConverter extends NumberDomainConverter[BigInt]("^[\\+\\-]?\\d+$") {
  override def mapValue(value: Any): Option[BigInt] = value match {
    case b: Byte => Some(b)
    case i: Int => Some(i)
    case f: Float => Some(f.toLong)
    case d: Double => Some(d.toLong)
    case s: Short => Some(s)
    case l: Long => Some(l)
    case bi: BigInt => Some(bi.longValue())
    case bd: BigDecimal => Some(bd.longValue())
    case jbi: JBigInteger => Some(jbi.longValue())
    case jdb: JBigDecimal => Some(jdb.longValue())
    case str: String => Try(BigInt(str)).toOption
    case _ => None
  }
}

/**
 * domain converter for target domains of type JBigInteger
 */
object JBigIntegerDomainConverter extends NumberDomainConverter[JBigInteger]("^[\\+\\-]?\\d+$") {
  override def mapValue(value: Any): Option[JBigInteger] = BigIntDomainConverter.mapValue(value).map(_.bigInteger)
}

/**
 * domain converter for target domains of type BigDecimal
 */
object BigDecimalDomainConverter extends NumberDomainConverter[BigDecimal]("^[\\+\\-]?(\\d+\\.)?\\d+(E[\\+\\-]?\\d+)?$") {
  override def mapValue(value: Any): Option[BigDecimal] = value match {
    case b: Byte => Some(b)
    case i: Int => Some(i)
    case f: Float => Some(f)
    case d: Double => Some(d)
    case s: Short => Some(s)
    case l: Long => Some(l)
    case bi: BigInt => Some(BigDecimal(bi))
    case bd: BigDecimal => Some(bd)
    case jbi: JBigInteger => Some(BigDecimal(jbi))
    case jdb: JBigDecimal => Some(jdb)
    case str: String => Try(BigDecimal(str)).toOption
    case _ => None
  }
}

/**
 * domain converter for target domains of type JBigDecimal
 */
object JBigDecimalDomainConverter extends NumberDomainConverter[JBigDecimal]("^[\\+\\-]?(\\d+\\.)?\\d+(E[\\+\\-]?\\d+)?$") {
  override def mapValue(value: Any): Option[JBigDecimal] = BigDecimalDomainConverter.mapValue(value).map(_.bigDecimal)
}
