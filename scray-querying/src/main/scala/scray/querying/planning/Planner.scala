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
package scray.querying.planning

import com.twitter.concurrent.Spool
import com.twitter.storehaus.QueryableStore
import com.twitter.storehaus.ReadableStore
import com.twitter.util.Await
import com.twitter.util.Future
import com.typesafe.scalalogging.slf4j.LazyLogging
import java.util.UUID
import scala.collection.mutable.HashMap
import scala.collection.parallel.immutable.ParSeq
import scray.querying.Query
import scray.querying.Registry
import scray.querying.description.{And, AtomicClause, Clause, Column, ColumnConfiguration, Columns, Equal, Greater, GreaterEqual, Unequal, Or, Row, Smaller, SmallerEqual, TableIdentifier}
import scray.querying.description.ColumnOrdering
import scray.querying.description.TableConfiguration
import scray.querying.description.internal.{Bound, Domain, NoPlanException, NonAtomicClauseException, QueryDomainParserException, QueryDomainParserExceptionReasons, QueryspaceViolationException, QueryWithoutColumnsException, RangeValueDomain, SingleValueDomain}
import scray.querying.description.internal.IndexTypeException
import scray.querying.description.internal.QueryspaceViolationTableUnavailableException
import scray.querying.description.internal.QueryspaceColumnViolationException
import scray.querying.description.internal.SingleValueDomain
import scray.querying.queries.DomainQuery
import scray.querying.source.EagerCollectingDomainFilterSource
import scray.querying.source.EagerEmptyRowDispenserSource
import scray.querying.source.EagerSource
import scray.querying.source.KeyValueSource
import scray.querying.source.LazyEmptyRowDispenserSource
import scray.querying.source.LazyQueryColumnDispenserSource
import scray.querying.source.LazyQueryDomainFilterSource
import scray.querying.source.LazySource
import scray.querying.source.OrderingEagerMappingSource
import scray.querying.source.QueryableSource
import scray.querying.source.SimpleHashJoinSource
import scray.querying.source.Source
import scray.querying.source.indexing.SimpleHashJoinConfig
import scray.querying.source.indexing.TimeIndexConfig
import scray.querying.source.indexing.TimeIndexSource
import scray.querying.description.internal.MaterializedView
import scala.annotation.tailrec
import scray.querying.source.ParallelizedQueryableSource
import scray.querying.description.internal._
import com.twitter.util.Try
import scray.querying.queries.QueryInformation

/**
 * Simple planner to execute queries.
 * For each query do:
 *   - transform query to a list of select-project-join queries
 *   - transform query predicates to a set of domains
 *   - find the main query
 */
object Planner extends LazyLogging {
  
  /**
   * plans the execution and starts it
   */
  def planAndExecute(query: Query): Spool[Row] = {
    
    logger.info(s"qid is ${query.getQueryID}")
    basicVerifyQuery(query)

    val queryInfo = Registry.createQueryInformation(query)
    
    // TODO: memoize query-plans if basicVerifyQuery has been successful
    
    val conjunctiveQueries = distributiveOrReductionToConjunctiveQuery(query)
    // from now on, we only have select-project-join / conjunctive queries
    // those can be executed in parallel and the union is returned 
    // if there is no orderby clause the queries can be pulled lazily, otherwise 
    // we need to do some merging
    
    // planning of conjunctive queries can be done in parallel
    val plans = conjunctiveQueries.par.map { cQuery => 
      // transform query into a query only containing domains
      val domainQuery = transformQueryDomains(cQuery)
    
      // find the main plan
      val plan = findMainQueryPlan(cQuery, domainQuery)
      
      // add in-memory filtering for rows which are excluded by the domains
      val filteredPlan = addRemainingFilters(plan, domainQuery)
      
      // add column removal for columns which are not needed any more
      val allColumns = cQuery.getResultSetColumns.columns.isLeft
      val dispensedColumnPlan = removeDispensableColumns(filteredPlan, domainQuery, allColumns)
      
      // remove empty rows
      val dispensedPlan = removeEmptyRows(dispensedColumnPlan, domainQuery, queryInfo)
      
      // if needed add an in-memory sorting step afterwards
      val executablePlan = sortedPlan(dispensedPlan, domainQuery)
      
      // post-actions
      Registry.queryPostProcessor(domainQuery, executablePlan)
      
      (executablePlan, domainQuery)
    }

    // do we need to order?
    val ordering = plans.find((execution) => execution._1.isInstanceOf[OrderedComposablePlan[_, _]]).
      map(_._1.asInstanceOf[OrderedComposablePlan[DomainQuery, _]])

    logger.debug(s"Plan computed for query: ${query.getQueryID.toString}")
      
    // run the plans and merge if it is needed
    MergingResultSpool.seekingLimitingSpoolTransformer(
        executePlans(plans.asInstanceOf[ParSeq[(OrderedComposablePlan[DomainQuery,_], DomainQuery)]], ordering == None, ordering),
        query.getQueryRange)
  }

  /**
   * verify basic query properties
   */
  @inline def basicVerifyQuery(query: Query): Unit = {
    // check that the queryspace is there
    Registry.getQuerySpace(query.getQueryspace).orElse(throw new QueryspaceViolationException(query))
    
    // check that the table is registered in the queryspace
    Registry.getQuerySpaceTable(query.getQueryspace, query.getTableIdentifier).orElse{
      throw new QueryspaceViolationException(query)
    }.map { tableConf =>
      if(!(tableConf.queryableStore.isDefined || tableConf.readableStore.isDefined)) {
          // check that there is a version for the table
          tableConf.versioned.orElse(throw new QueryspaceViolationTableUnavailableException(query)). 
            map(_.runtimeVersion().orElse(throw new QueryspaceViolationTableUnavailableException(query)))
      }
    }
    
    // def to throw if a column is not registered 
    @inline def checkColumnReference(reference: Column): Unit = {
      Registry.getQuerySpaceColumn(query.getQueryspace, reference) match {
        case None => throw new QueryspaceColumnViolationException(query, reference)
        case _ => // column is registered, o.k.
      }
    }
    
    // check that all queried columns are registered
    query.getResultSetColumns.columns match {
      case Right(columns) => columns.foreach(col => checkColumnReference(col))
      case Left(bool) => if(bool) { /* is a star (*), o.k. */ } else {
        throw new QueryWithoutColumnsException(query)
      }
    }
    
    // check that all columns used in clauses are registered
    query.getWhereAST.foreach { 
      case equal: Equal[_] => checkColumnReference(equal.column)
      case greater: Greater[_] => checkColumnReference(greater.column)
      case greaterequal: GreaterEqual[_] => checkColumnReference(greaterequal.column)
      case smaller: Smaller[_] => checkColumnReference(smaller.column)
      case smallerequal: SmallerEqual[_] => checkColumnReference(smallerequal.column)
      case unequal: Unequal[_] => checkColumnReference(unequal.column)
      case _ => // do not need to check, not an atomic clause
    }
    
    // check that all referenced columns in orderby, groupby are registered
    query.getGrouping.map(grouping => checkColumnReference(grouping.column))
    query.getOrdering.map(ordering => checkColumnReference(ordering.column))
  }
  
  /**
   * make a cartesian product of the provided List of Lists
   */
  protected def cartesianClauseProduct(l: List[List[Clause]]): List[List[Clause]] = {
    if(l.size == 0) {
      List(List[Clause]())
    } else {
      l.head.flatMap(item => cartesianClauseProduct(l.tail).map(product => item :: product))
    }
  }
  
  /**
   * in the end this shall return a list of flattened and-terms with
   * clauses only contained of atomic clauses
   */
  protected def distributiveOrReductionOnClause(clause: Clause): List[Clause] = {
    clause match {
      case or: Or => {
        // split n clauses into n disjoint clauses
        or.flatten.clauses.toList.flatMap(orClausePart => distributiveOrReductionOnClause(orClausePart))
      }
      case and: And => {
        // first reduce sub-terms to produce a List of Lists
        val orLists = and.clauses.toList.map(distributiveOrReductionOnClause(_))
        // now we "multiply" by creating new Ands by making this a cartesian product
        val cartesian = cartesianClauseProduct(orLists)
        // now we eliminate direct sub-ands from the lists and make each of them an and
        cartesian.map(list => new And(list:_*).flatten)
      }
      case _:AtomicClause => List(clause) // this is an atomic clause
      case _ => List(clause)
    }
  }
  
  /**
   * use distributive law to remove ors and produce a conjunctive queries.
   * This may or may not be a good idea for special cases at hand, but we leave
   * this for optimization in the future. 
   */
  def distributiveOrReductionToConjunctiveQuery(query: Query): List[Query] = {
    query.getWhereAST.map { 
      ast => distributiveOrReductionOnClause(ast).map(clause => query.transformedAstCopy(Some(clause)))
    }.getOrElse(List(query))
  }
  
  /**
   * Checks that we can use a one or more materialized views for a given query.
   * If there is more than one view available the result will be the one with the "best"
   * (i.e. highest) score (number of matching columns), except if some of the views
   * support the ordering defined, then the "best" of this sub-set be used regardless
   * of other views with better scores (saves memory).
   */
  private def checkMaterializedViewMatching(space: String, table: TableIdentifier, domQuery: DomainQuery): Option[(Boolean, MaterializedView)] = {
    // check that all 
    @inline def checkSingleValueDomainValues(column: Column, values: Array[SingleValueDomain[_]]): Boolean = {
      values.find { singleVDom => 
        domQuery.domains.find { dom => 
          dom.column == column && (dom match {
            case single: SingleValueDomain[_] => singleVDom.value == single.value
            case _ => false
          })
        }.isDefined
      }.isDefined
    }
    @inline def checkRangeValueDomainValues(column: Column, values: Array[RangeValueDomain[_]]): Boolean = {
      values.find { rangeDom => 
        domQuery.domains.find { dom => 
          dom.column == column && (dom match {
            case range: RangeValueDomain[_] => range.asInstanceOf[RangeValueDomain[Any]].
              isSubIntervalOf(rangeDom.asInstanceOf[RangeValueDomain[Any]])
            case _ => false
          })
        }.isDefined
      }.isDefined
    }
    @tailrec def findMaterializedViews(views: List[MaterializedView],  
                                          resultViews: List[(Boolean, Int, MaterializedView)]): 
                                          List[(Boolean, Int, MaterializedView)] = {
      if(views.isEmpty) {
        resultViews
      } else {
        val matView = views.head
        // but... do we have constraints? If yes, check these first.
        val moreThanZero = (!matView.fixedDomains.isEmpty) || (!matView.rangeDomains.isEmpty)
        val matOpt: Option[(Boolean, Int)] = if(moreThanZero) {
          // if we don't find a Domain of the view that doesn't match we found a usable view        
          val fdom = matView.fixedDomains.find((mat) => !checkSingleValueDomainValues(mat._1, mat._2)).isEmpty
          val rdom = matView.rangeDomains.find((mat) => !checkRangeValueDomainValues(mat._1, mat._2)).isEmpty
          if(fdom && rdom) {
            matView.checkMaterializedView(matView, domQuery)
          } else {
            None
          }
        } else {
          // no constraints return whether view is matching and how
          matView.checkMaterializedView(matView, domQuery)
        }
        val resultlist = matOpt match {
          case Some(addMatView) => (addMatView._1, addMatView._2, matView) :: resultViews 
          case None => resultViews
        }
        findMaterializedViews(views.tail, resultlist)
      }
    }
    Registry.getQuerySpaceTable(space, table).flatMap { config =>
      val views = findMaterializedViews(config.materializedViews, Nil)
      if(!views.isEmpty) {
        val orderedViews = views.filter(_._1)
        if(!orderedViews.isEmpty) {
          Some((true, orderedViews.maxBy[Int](_._2)._3))
        } else {
          Some((false, views.maxBy[Int](_._2)._3))
        }
      } else {
        None
      }
    }
  }

  /**
   * returns a QueryableStore and uses versioning information, if needed
   */
  def getQueryableStore[Q, K, V](tableConfig: TableConfiguration[Q, K, V]): QueryableStore[Q, V] = tableConfig.versioned match {
    case None => tableConfig.queryableStore.get()
    case Some(versionInfo) => 
      logger.info(s"requesting store with ${versionInfo.runtimeVersion().get}")
      versionInfo.queryableStore(versionInfo.runtimeVersion().get)
  }

  /**
   * returns a ReadableStore and uses versioning information, if needed
   */
  def getReadableStore[Q, K, V](tableConfig: TableConfiguration[Q, K, V]): ReadableStore[K, V] = tableConfig.versioned match {
    case None => tableConfig.readableStore.get()
    case Some(versionInfo) => versionInfo.readableStore(versionInfo.runtimeVersion().get)
  }
  
  /**
   * Finds the main query
   * 
   * 1. check for order by
   * 2. check for group by
   * 3. perform filter resolution
   */
  def findMainQueryPlan[T](query: Query, domainQuery: DomainQuery): ComposablePlan[DomainQuery, _] = {
    // 1. check if we have a matching materialized view prepared and 
    // whether it is ordered according to our ordering: Option[(ordered: Boolean, table)] 
    checkMaterializedViewMatching(query.getQueryspace, query.getTableIdentifier, domainQuery) match {
      case Some((ordered, viewConf)) =>
        val qSource = new QueryableSource(getQueryableStore(viewConf.viewTable), query.getQueryspace, domainQuery.table, ordered) 
        return ComposablePlan.getComposablePlan(qSource, domainQuery)
      case _ =>
    }
 
    // 2. materialized views aren't available for this query. Build non-view-based plan 
    val sortedColumnConfig: Option[ColumnConfiguration] = query.getOrdering.flatMap { _ => 
      // we shall sort - do we have sorting in the query space?
      Registry.getQuerySpace(query.getQueryspace).flatMap(_.queryCanBeOrdered(query))
    }

    val groupedColumnConfig: Option[ColumnConfiguration] = query.getOrdering.flatMap { _ => 
      // we shall group - do we have auto-grouping?
      Registry.getQuerySpace(query.getQueryspace).flatMap(_.queryCanBeGrouped(query))
    }

    // if we do not have a sorting nor a grouping, we try to find the first hand-made index
    val mainColumn: Option[ColumnConfiguration] = sortedColumnConfig.orElse(groupedColumnConfig).orElse{
      domainQuery.domains.map { domain =>
        Registry.getQuerySpaceColumn(query.getQueryspace, domain.column).flatMap{col => 
          if(col.index.map(_.isManuallyIndexed.isDefined).getOrElse(false)) {
            Some(col)
          } else {
            None
          }
        }
      }.find(_.isDefined).getOrElse(None)
    }

    // construct a simple plan
    mainColumn.map { colConf =>
      colConf.index.flatMap(index => index.isManuallyIndexed.map { tableConf =>
        val indexTableConfig = tableConf.indexTableConfig()
        val mainTableConfig = tableConf.mainTableConfig()
        val indexSource = new QueryableSource(getQueryableStore(indexTableConfig),
          query.getQueryspace, indexTableConfig.table, index.isSorted)
        val mainSource = new KeyValueSource(getReadableStore(mainTableConfig), 
          query.getQueryspace, mainTableConfig.table, Registry.getCachingEnabled)
        tableConf.indexConfig match {
          case simple: SimpleHashJoinConfig => new SimpleHashJoinSource(indexSource, colConf.column, 
            mainSource, mainTableConfig.primarykeyColumns)
          case time: TimeIndexConfig =>
            // maybe a parallel version is available --> convert to parallel version
            val timeQueryableSource = time.parallelization match {
              // case Some(parFunc) => indexSource
              case Some(parFunc) => new ParallelizedQueryableSource(indexSource.store, indexSource.space, 
                      time.parallelizationColumn.get, parFunc(indexSource.store), time.ordering, 
                      query.getOrdering.filter(_.descending).isDefined)
              case None => indexSource
            }
            new TimeIndexSource(time, timeQueryableSource, mainSource.asInstanceOf[KeyValueSource[Any, _]], 
                                mainTableConfig.table, tableConf.keymapper,
                                time.parallelization.flatMap(_(getQueryableStore(indexTableConfig))))
          case _ => throw new IndexTypeException(query)
        }
      }).orElse {
        Registry.getQuerySpaceTable(domainQuery.getQueryspace, domainQuery.getTableIdentifier).map { tableConf =>
          new QueryableSource(getQueryableStore(tableConf), query.getQueryspace, tableConf.table, true)
        }
      }
    }.getOrElse {
      // construct plan using information on main table
      Registry.getQuerySpaceTable(domainQuery.getQueryspace, domainQuery.getTableIdentifier).map { tableConf =>
        new QueryableSource(getQueryableStore(tableConf), query.getQueryspace, tableConf.table)
      }
    }.map(ComposablePlan.getComposablePlan(_, domainQuery)).getOrElse(throw new NoPlanException(query))
  }

  /**
   * intersect domains for a single predicate of a query
   */
  @inline private def domainComparator[T](query: Query,
                                          col: Column,
                                          throwFunc: (T) => Boolean,
                                          creationDomain: RangeValueDomain[T], collector: HashMap[Column, Domain[_]]): Unit = {
    collector.get(col).map { _ match {
        case equal: SingleValueDomain[T] => if (throwFunc(equal.value)) {
          throw new QueryDomainParserException(QueryDomainParserExceptionReasons.DOMAIN_EQUALITY_CONFLICT, col, query)
        }
        case range: RangeValueDomain[T] => {
          collector.put(col, range.bisect(creationDomain)
            .getOrElse(throw new QueryDomainParserException(QueryDomainParserExceptionReasons.DOMAIN_DISJOINT_CONFLICT, col, query)))
        }
        case _ => throw new QueryDomainParserException(QueryDomainParserExceptionReasons.UNKNOWN_DOMAIN_CONFLICT, col, query)
      }
    }.orElse {
      collector.put(col, creationDomain)
    }
  }
  
  /**
   * Maps a select-project-join query to a list of domains.
   * Throws an Exception if the query does not only consist of select-project-join queries.
   */
  def qualifyPredicates(query: Query): Option[List[Domain[_]]] = {
    def qualifySinglePredicate[T](clause: AtomicClause, collector: HashMap[Column, Domain[_]]): Unit = clause match {
      case e: Equal[T] => {
        collector.get(e.column).map { pred => pred match {
          // this is only allowed, if it either is the same value or pred is a range in which case we reduce to equal
          case equal: SingleValueDomain[T] => if(!e.equiv.equiv(e.value, equal.value)) { 
            throw new QueryDomainParserException(QueryDomainParserExceptionReasons.DISJOINT_EQUALITY_CONFLICT, e.column, query) }
          case range: RangeValueDomain[T] => if(range.valueIsInBounds(e.value)) { collector.put(e.column, SingleValueDomain(e.column, e.value))
            } else { throw new QueryDomainParserException(QueryDomainParserExceptionReasons.DOMAIN_EQUALITY_CONFLICT, e.column, query) }
          case _ => throw new QueryDomainParserException(QueryDomainParserExceptionReasons.UNKNOWN_DOMAIN_CONFLICT, e.column, query)
        }}.orElse {
          collector.put(e.column, SingleValueDomain(e.column, e.value))
        }
      }
      case g: Greater[T] => domainComparator[T](query,
          g.column, 
          value => { g.ordering.compare(g.value, value) >= 0 },
          RangeValueDomain(g.column, Some(Bound[T](false, g.value)(g.ordering)), None)(g.ordering),
          collector)
      case ge: GreaterEqual[T] => domainComparator[T](query,
          ge.column, 
          value => { ge.ordering.compare(ge.value, value) > 0 },
          RangeValueDomain(ge.column, Some(Bound[T](true, ge.value)(ge.ordering)), None)(ge.ordering),
          collector)
      case l: Smaller[T] => domainComparator[T](query,
          l.column, 
          value => { l.ordering.compare(l.value, value) <= 0 },
          RangeValueDomain(l.column, None, Some(Bound(false, l.value)(l.ordering)))(l.ordering),
          collector)
      case le: SmallerEqual[T] => domainComparator[T](query,
          le.column, 
          value => { le.ordering.compare(le.value, value) < 0 },
          RangeValueDomain(le.column, None, Some(Bound(true, le.value)(le.ordering)))(le.ordering),
          collector)
      case ne: Unequal[T] => domainComparator[T](query,
          ne.column, 
          value => { ne.ordering.compare(ne.value, value) == 0 },
          new RangeValueDomain(ne.column, List(ne.value))(ne.ordering),
          collector)
    }
    // collect all predicates where columns are the same and try to define domains
    val collector = new HashMap[Column, Domain[_]]
    query.getWhereAST.map ( _ match {
      case atomic: AtomicClause => qualifySinglePredicate(atomic, collector)
      case and: And => and.clauses.map { clause => 
        // after a select-project-join transformation we can safely assume only AtomicClauses
        qualifySinglePredicate(clause.asInstanceOf[AtomicClause], collector)
      }
      case _ => throw new NonAtomicClauseException(query)
    })
    if(collector.size == 0) None else Some(collector.values.seq.toList)
  }
  
  /**
   * identifies columns which are being queried.
   * TODO: add functions that can be queried
   */
  @inline def identifyColumns(list: Columns, table: TableIdentifier, domains: List[Domain[_]], query: Query): List[Column] = {
    list.columns match {
      case Right(cols) => cols
      case Left(all) => { // the result will be all possible columns, i.e. all from the table + columns from domains
        if(all) {
          (Registry.getQuerySpaceTable(query.getQueryspace, table).getOrElse {
              throw new QueryspaceViolationException(query)
            }.allColumns.toSet ++ domains.map(dom => dom.column).toSet).toList
        } else { throw new QueryWithoutColumnsException(query) }
      }
    }
  }
  
  /**
   * transforms a predicate-based query into a (internal) domain-based one
   */
  @inline def transformQueryDomains(query: Query): DomainQuery = {
    val domains = qualifyPredicates(query).getOrElse(List())
    val table = query.getTableIdentifier
    DomainQuery(query.getQueryID,
        query.getQueryspace, 
        identifyColumns(query.getResultSetColumns, table, domains, query),
        table,
        domains,
        query.getGrouping,
        query.getOrdering,
        query.getQueryRange)
  }

  /**
   * Filters rows that do not satisfy filter criterias
   * TODO: later only check filters that have not been used by database
   */
  def addRemainingFilters(plan: ComposablePlan[DomainQuery, _], domainQuery: DomainQuery): ComposablePlan[DomainQuery, _] = {
    plan.getSource match {
      case lazySource: LazySource[_] => ComposablePlan.getComposablePlan(
          new LazyQueryDomainFilterSource(lazySource), domainQuery)
      case eagerSource: EagerSource[_] => ComposablePlan.getComposablePlan(
          new EagerCollectingDomainFilterSource[DomainQuery, Seq[Row]](
              eagerSource.asInstanceOf[Source[DomainQuery, Seq[Row]]]), domainQuery)
    }
  }
      
  /**
   * Add column removal for columns which are not needed any more to the plan
   */
  def removeDispensableColumns(filteredPlan: ComposablePlan[DomainQuery, _],
      domainQuery: DomainQuery, allColumns: Boolean): ComposablePlan[DomainQuery, _] = {
    if(!allColumns) {
      filteredPlan.getSource match {
        case lazySource: LazySource[_] => ComposablePlan.getComposablePlan(
            new LazyQueryColumnDispenserSource(lazySource), domainQuery)
        case eagerSource: EagerSource[_] => ComposablePlan.getComposablePlan(
          new EagerCollectingDomainFilterSource[DomainQuery, Seq[Row]](
              eagerSource.asInstanceOf[Source[DomainQuery, Seq[Row]]]), domainQuery)
      }
    } else {
      filteredPlan
    }
  }

  /**
   * Add row removal for rows which are empty
   */
  def removeEmptyRows(filteredPlan: ComposablePlan[DomainQuery, _], domainQuery: DomainQuery, 
                      queryInfo: QueryInformation): ComposablePlan[DomainQuery, _] = {
    filteredPlan.getSource match {
      case lazySource: LazySource[_] => ComposablePlan.getComposablePlan(
          new LazyEmptyRowDispenserSource(lazySource, Some(queryInfo)), domainQuery)
      case eagerSource: EagerSource[_] => ComposablePlan.getComposablePlan(
          new EagerEmptyRowDispenserSource[DomainQuery, Seq[Row]](
              eagerSource.asInstanceOf[Source[DomainQuery, Seq[Row]]], Some(queryInfo)), domainQuery) 
    }
  }
  
  /**
   * if needed add an in-memory sorting step afterwards
   */ 
  def sortedPlan(dispensedPlan: ComposablePlan[DomainQuery, _], domainQuery: DomainQuery): ComposablePlan[DomainQuery, _] = {
    dispensedPlan match {
      case ocp: OrderedComposablePlan[DomainQuery, _] => if(ocp.getSource.isOrdered(domainQuery)) ocp else {
        val source = ocp.getSource match {
          case lazySource: LazySource[_] => new OrderingEagerMappingSource[DomainQuery, Spool[Row]](
              lazySource.asInstanceOf[Source[DomainQuery, Spool[Row]]])
          case eagerSource: EagerSource[_] => new OrderingEagerMappingSource[DomainQuery, Seq[Row]](
              eagerSource.asInstanceOf[Source[DomainQuery, Seq[Row]]]) 
        }
        new OrderedComposablePlan(source, domainQuery.getOrdering)
      }
      case ucp: UnorderedComposablePlan[_, _] => ucp
    }
  }
  
  /**
   * executes the plan, and returns the Futures to be returned by the engine
   * and which might still need to be merged
   */
  def executePlans(plans: ParSeq[(ComposablePlan[DomainQuery, _], DomainQuery)], 
      unOrdered: Boolean,
      ordering: Option[OrderedComposablePlan[DomainQuery, _]]): Spool[Row] = {
    // run all at once
    val t1 = System.currentTimeMillis()
    val futures = plans.par.map { (execution) => 
      val source = execution._1.getSource
      (source.isLazy, source.request(execution._2))
    }.seq

    if(futures.size == 0) {
      Spool.Empty.asInstanceOf[Spool[Row]]
    } else if(futures.size == 1) {
      // in case we only have results for one query we can quickly return them
      Await.result(futures.head._2) match {
        case spool: Spool[_] => spool.asInstanceOf[Spool[Row]]
        case seq: Seq[_] => Spool.seqToSpool[Row](seq.asInstanceOf[Seq[Row]]).toSpool
      }
    } else { 
      val seqtuple = futures.partition ( future => future._1 )
      val seqs = seqtuple._2.map(_._2).asInstanceOf[Seq[Future[Seq[Row]]]]
      val spools = seqtuple._1.map(_._2).asInstanceOf[Seq[Future[Spool[Row]]]]
      val indexes = Seq.fill(seqs.size)(0)      
      unOrdered match {
        case true =>
          // we spit out the value which is available first and so on
          Await.result(MergingResultSpool.mergeUnorderedResults(seqs, spools, indexes))
        case false =>
          // we wait for all data streams to return a result and then return the lowest value
          val compRows: (Row, Row) => Boolean = 
            scray.querying.source.rowCompWithOrdering(ordering.get.ordering.get.column, 
                                                      ordering.get.ordering.get.ordering,
                                                      ordering.get.ordering.get.descending)
          Await.result(MergingResultSpool.mergeOrderedSpools(seqs, spools, compRows, ordering.get.ordering.get.descending, indexes))
      }
    }
  }
}
