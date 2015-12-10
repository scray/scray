package scray.hesse.generator.hadoop

import java.util.ArrayList
import java.util.List
import scray.hesse.generator.GeneratorError
import scray.hesse.generator.GeneratorState
import scray.hesse.generator.HeaderInformation
import scray.hesse.generator.StaticUtilities
import scray.hesse.generator.hadoop.HesseFunctionGenerator.Parameter
import scray.hesse.hesseDSL.AbstractResultColumnDefinition
import scray.hesse.hesseDSL.AndPredicates
import scray.hesse.hesseDSL.BinaryPredicateComparator
import scray.hesse.hesseDSL.BodyStatement
import scray.hesse.hesseDSL.BracketPredicate
import scray.hesse.hesseDSL.ComparatorPredicate
import scray.hesse.hesseDSL.ConstantColumn
import scray.hesse.hesseDSL.FunctionColumn
import scray.hesse.hesseDSL.MaterializedViewStatement
import scray.hesse.hesseDSL.NegationPredicate
import scray.hesse.hesseDSL.OrPredicates
import scray.hesse.hesseDSL.SelectColumn
import scray.hesse.hesseDSL.UnaryPredicateComparator
import org.eclipse.xtend.lib.annotations.Data

/**
 * Implements all predicate stuff within the mappers
 */
class HesseHadoopMapperFiltering {

	// used to handle more complex function stuff
	val functionGenerator = new HesseFunctionGenerator
	
	/**
	 * evaluates a function or column and may append to the current buffer
	 * The result is used as a variable name or content, later on
	 */
	private def Parameter evaluateColumnOrFunction(AbstractResultColumnDefinition abscol, StringBuffer buffer, GeneratorState state, HeaderInformation header, MaterializedViewStatement view, List<BodyStatement> bodyStatements) {
		switch(abscol) {
			FunctionColumn: {
				functionGenerator.generateFunction(true, abscol, buffer, state, header, view, bodyStatements)
			}
			ConstantColumn: {
				HesseColumnGenerator::generateConstantColumnSpecification(abscol)
			}
			SelectColumn: {
				HesseColumnGenerator::generateRowColumnSpecification(abscol, buffer, state)
			}
			default: {
				throw new GeneratorError("Column type is unknown: " + abscol.getClass.getName)
			}
		} 
	}
	
	/**
	 * Generate code that executes a unary predicate comparison
	 * Return type of the returned variable name is always Boolean
	 */
	private def String recurseUnaryPredicate(ComparatorPredicate predicate, StringBuffer buffer, GeneratorState state, HeaderInformation header, MaterializedViewStatement view, List<BodyStatement> bodyStatements) {
		val unaryPredicateComparator = predicate.name as UnaryPredicateComparator
		val columnStuff = evaluateColumnOrFunction(predicate.left, buffer, state, header, view, bodyStatements)
		val varname = StaticUtilities::createVarDeclaration(buffer, "unary", state)
		// predicate
		buffer.append(columnStuff.varname)
		// Right now, unary predicates can only be "is not null" or "is null"
		if(unaryPredicateComparator.name.toLowerCase.contains("not")) {
			buffer.append(''' != null''')
		} else {
			buffer.append(''' == null''')
		}
		buffer.append(System::lineSeparator)
		varname
	}
	
	/**
	 * Generate code that executes a binary predicate comparison
	 * Return type of the returned variable name is always Boolean
	 */
	private def String recurseBinaryPredicate(ComparatorPredicate predicate, StringBuffer buffer, GeneratorState state, HeaderInformation header, MaterializedViewStatement view, List<BodyStatement> bodyStatements) {
		// normalize both sides to a common but similar type, that can be compared
		val binaryPredicateComparator = predicate.name as BinaryPredicateComparator
		val columnLeft = evaluateColumnOrFunction(predicate.left, buffer, state, header, view, bodyStatements)
		val columnRight = evaluateColumnOrFunction(predicate.right, buffer, state, header, view, bodyStatements)		
		val varname = StaticUtilities::createVarDeclaration(buffer, "binary", state)
		buffer.append(columnLeft.varname)
		buffer.append(" ")
		switch(binaryPredicateComparator.name) {
			case EQUAL: buffer.append("==")
			case UNEQUAL: buffer.append("!=")
			case LIKE: throw new GeneratorError("LIKE not supported by binary predicate evaluation, yet")
			default: {
				buffer.append(binaryPredicateComparator.name)
			}
		}
		buffer.append(" ")
		buffer.append(columnRight.varname)
		buffer.append(System::lineSeparator)
		varname
	}
	
	/**
	 * generates code to evaluate inversed (NOT) sub-predicates
	 */
	private def String recurseNegation(NegationPredicate predicate, StringBuffer buffer, GeneratorState state, HeaderInformation header, MaterializedViewStatement view, List<BodyStatement> bodyStatements) {
		val orVarName = recurseOrPredicate(predicate.predicate, buffer, state, header, view, bodyStatements)
		val varname = StaticUtilities::createVarDeclaration(buffer, "negation", state)
		buffer.append(" ! ")
		buffer.append(orVarName)
		buffer.append(System::lineSeparator)
		varname
	}
	
	/**
	 * recurse down the AND and evaluate all sub-predicates as well as the combination of all by &&
	 */
	private def String recurseAndPredicate(AndPredicates predicate, StringBuffer buffer, GeneratorState state, HeaderInformation header, MaterializedViewStatement view, List<BodyStatement> bodyStatements) {
		val vars = new ArrayList(predicate.predicates.map [ pred | 
			switch(pred) {
				ComparatorPredicate: {
					if(StaticUtilities::isSet(pred, "right")) {
						recurseBinaryPredicate(pred, buffer, state, header, view, bodyStatements)
					} else {
						recurseUnaryPredicate(pred, buffer, state, header, view, bodyStatements)
					}
				}
				NegationPredicate: recurseNegation(pred, buffer, state, header, view, bodyStatements)
				BracketPredicate: recurseOrPredicate(pred.predicate, buffer, state, header, view, bodyStatements)
				default: {
					throw new GeneratorError("And-Predicate sub-type is unknown: " + predicate.getClass.getName)
				}
			}		
		])
		val varname = StaticUtilities::createVarDeclaration(buffer, "and", state)
		buffer.append(vars.join(" && "))
		buffer.append(System::lineSeparator)
		varname
	}
	
	/**
	 * recurse down the OR and evaluate all sub-predicates as well as the combination of all by ||
	 */
	private def String recurseOrPredicate(OrPredicates predicate, StringBuffer buffer, GeneratorState state, HeaderInformation header, MaterializedViewStatement view, List<BodyStatement> bodyStatements) {
		val vars = new ArrayList(predicate.predicates.map [ pred | 
			recurseAndPredicate(pred, buffer, state, header, view, bodyStatements)
		])
		val varname = StaticUtilities::createVarDeclaration(buffer, "or", state)
		buffer.append(vars.join(" || "))
		buffer.append(System::lineSeparator)
		varname
	} 

	/**
	 * return type for returning a bunch of code and a variable name 
	 */
	@Data static class VarAndFilters {
		String varname
		String code
	}

	/**
	 * creates the filtering definition in the mapper
	 */
	def VarAndFilters generateFilteringAndFunctionApplication(GeneratorState state, HeaderInformation header, MaterializedViewStatement view, List<BodyStatement> bodyStatements) {
		var result = new StringBuffer;
		state.resetNextVarNumber
		val varname = if(StaticUtilities::isSet(view.select, "filters")) {
			// strategy:
			// 1. we recurse down until we find a binary or an unary predicate
			// 2. we evaluate the predicate into a variable
			// 3. on each sub-layer, we combine all variables according to the bools operators
			// 4. evaluate if the row has been "accepted" or not
			recurseOrPredicate(view.select.filters.predicates, result, state, header, view, bodyStatements)
		}
		new VarAndFilters(varname, result.toString)
	}
}