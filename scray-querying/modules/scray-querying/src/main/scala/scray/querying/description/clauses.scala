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
package scray.querying.description

import java.util.regex.Pattern

/**
 * a prefixed-declaration of a simple query-clause AST
 */
sealed trait Clause

/**
 * represents ORs between all the provided clauses
 * empty clauses are being ignored
 */
case class Or(clauses: Clause*) extends Clause {
  def flatten: Or = {
    new Or(clauses.flatMap { clause =>
      clause match {
        case or: Or => {
          or.clauses
        }
        case _ => List(clause)
      }
    }:_*)
  }
}

/**
 * represents ANDs between all the provided clauses
 * empty clauses are being ignored
 */
case class And(clauses: Clause*) extends Clause {
  def flatten: And = {
    new And(clauses.flatMap { clause =>
      clause match {
        case and: And => {
          and.flatten.clauses
        }
        case _ => List(clause)
      }
    }:_*)
  }
}

/**
 * Represents class of atomic clauses.
 * All atomic clauses with obvious purpose are following
 */
sealed trait AtomicClause extends Clause
case class Equal[T](column: Column, value: T)(implicit val equiv: Equiv[T]) extends AtomicClause
case class Greater[T](column: Column, value: T)(implicit val ordering: Ordering[T]) extends AtomicClause
case class GreaterEqual[T](column: Column, value: T)(implicit val ordering: Ordering[T]) extends AtomicClause
case class Smaller[T](column: Column, value: T)(implicit val ordering: Ordering[T]) extends AtomicClause
case class SmallerEqual[T](column: Column, value: T)(implicit val ordering: Ordering[T]) extends AtomicClause
case class Unequal[T](column: Column, value: T)(implicit val ordering: Ordering[T]) extends AtomicClause
case class IsNull[T](column: Column) extends AtomicClause
case class Wildcard[T <: String](column: Column, value: T) extends AtomicClause

object WildcardChecker {
  /**
   * check if a value is matching a given wildcard string
   */
  def checkValueAgainstPredicate[T <: String](that: T, other: T): Boolean = {
    val pattern = Pattern.compile(that.replaceAll("\\*", ".*").replaceAll("\\\\.\\*", "\\\\*").replaceAll("\\?", ".?").replaceAll("\\\\.\\?", "\\\\?"))
    pattern.matcher(other).matches()
  }
}

