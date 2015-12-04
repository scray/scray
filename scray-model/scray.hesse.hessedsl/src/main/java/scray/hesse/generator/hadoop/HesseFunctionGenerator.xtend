package scray.hesse.generator.hadoop

import java.util.ArrayList
import java.util.Calendar
import java.util.List
import scray.hesse.generator.GeneratorError
import scray.hesse.generator.GeneratorState
import scray.hesse.generator.HeaderInformation
import scray.hesse.hesseDSL.AbstractTerminalColumnDefinition
import scray.hesse.hesseDSL.BodyStatement
import scray.hesse.hesseDSL.BuiltinAggFunction
import scray.hesse.hesseDSL.BuiltinColumnFunction
import scray.hesse.hesseDSL.BuiltinColumnFunctions
import scray.hesse.hesseDSL.ConstantColumn
import scray.hesse.hesseDSL.FunctionColumn
import scray.hesse.hesseDSL.MaterializedViewStatement
import scray.hesse.hesseDSL.SelectColumn

/**
 * generate function executions
 */
class HesseFunctionGenerator {
	
	List<String> warnings = new ArrayList<String>();
	
	def Parameter generateFunction(boolean where, FunctionColumn function, StringBuffer buffer, GeneratorState state, HeaderInformation header, MaterializedViewStatement view, List<BodyStatement> bodyStatements) {
		val params = generateParameters(where, function, buffer, state, header, view, bodyStatements)
		if(function.function instanceof BuiltinAggFunction) {
			// aggregation functions are not allowed in where clauses
			if(where) {
				throw new GeneratorError("SQL aggregation functions are not allowed in WHERE clauses")
			} 
			generateBuiltinAggregationFunction(function.function as BuiltinAggFunction, params, buffer, state, header, view, bodyStatements)
		} else if(function.function instanceof BuiltinColumnFunction) {
			generateBuiltinColumnFunction(function.function as BuiltinColumnFunction, params, buffer, state, header, view, bodyStatements)
		}
	}
	
	/**
	 * container class to pass around parameters
	 */
	public static class Parameter {
		public val String varname
		public val Types vartype
		new(String name, Types type) {
			varname = name
			vartype = type
		}
	}
	
	/**
	 * types which can be set
	 */
	public static enum Types {
		Any, String, Int, Long, BigInt, BigDecimal, Double, Timestamp, Float, Date, ByteArray;
	}
	
	/**
	 * generates code that parses the parameters
	 */
	private def List<Parameter> generateParameters(boolean where, FunctionColumn function, StringBuffer buffer, GeneratorState state, HeaderInformation header, MaterializedViewStatement view, List<BodyStatement> bodyStatements) {
		function.parameters.map [ parameter |
			switch(parameter) {
				FunctionColumn: generateFunction(where, parameter, buffer, state, header, view, bodyStatements)
				AbstractTerminalColumnDefinition: generateTerminalColumn(parameter, buffer, state)
				default: {
					throw new GeneratorError("Function-Parameter-Definition type is unknown: " + parameter.getClass.getName)				
				}
			}
		]
	}

	/**
	 * generates terminal parameter code
	 */
	private def Parameter generateTerminalColumn(AbstractTerminalColumnDefinition coldef, StringBuffer buffer, GeneratorState state) {
		switch(coldef) {
			ConstantColumn: HesseColumnGenerator::generateConstantColumnSpecification(coldef)
			SelectColumn: HesseColumnGenerator::generateRowColumnSpecification(coldef, buffer, state)
			default: {
				throw new GeneratorError("Column-Definition type is unknown: " + coldef.getClass.getName)
			}
		}
	}
	
	/**
	 * generate implementations for built in aggregate functions
	 */
	private def generateBuiltinAggregationFunction(BuiltinAggFunction function, List<Parameter> paramVars, StringBuffer buffer, GeneratorState state, HeaderInformation header, MaterializedViewStatement view, List<BodyStatement> bodyStatements) {
		// TODO: implement the aggregator functions
		new Parameter("dummy", Types.Any)
	}
	
	/**
	 * internal function to handle date projections
	 */
	private def Parameter generateTimeGetter(String varname, int field, List<Parameter> paramVars, String modifier, StringBuffer buffer, HeaderInformation header, MaterializedViewStatement view) {
		if(paramVars.get(0).vartype != Types.Any && paramVars.get(0).vartype != Types.Date && paramVars.get(0).vartype != Types.Long && paramVars.get(0).vartype != Types.Int && paramVars.get(0).vartype != Types.String) {
			warnings.add("Type " + paramVars.get(0).vartype + " not expected for retrieving date fields. Assuming String.")
		}
		buffer.append("val " + varname + " = " + paramVars.get(0).varname + " match {")
		buffer.append(System.lineSeparator)
		buffer.append("  case d: Date => " + HesseHadoopLibraryGenerator::getLib(header, view) + ".extractTime(d, " + field + ")" + modifier)
		buffer.append(System.lineSeparator)
		// assume the long to be a Unix-Epoch time
		buffer.append("  case l: Long => " + HesseHadoopLibraryGenerator::getLib(header, view) + ".extractTime(new Date(l), " + field + ")" + modifier)
		buffer.append(System.lineSeparator)
		// assume the int to be an old Unix timestamp in seconds
		buffer.append("  case i: Int => " + HesseHadoopLibraryGenerator::getLib(header, view) + ".extractTime(new Date(i.toLong * 1000L), " + field + ")" + modifier)
		buffer.append(System.lineSeparator)
		// default is to assume its a string; this is a difficult matter...
		// is it a Number/Long? -> Unix-Epoch time
		// is it of format "YYYY-MM-DD..."? -> make a string date
		buffer.append("  case s => val str = s.toString")
		buffer.append(System.lineSeparator)
		buffer.append("    Try(" + HesseHadoopLibraryGenerator::getLib(header, view) + ".extractTime(new Date(str.toLong), " + field + ")" + modifier + ").getOrElse {")
		buffer.append(System.lineSeparator)
		buffer.append("      Try(" + HesseHadoopLibraryGenerator::getLib(header, view) + ".extractTime(" + HesseHadoopLibraryGenerator::getLib(header, view) + ".stringToDate(str), " + field + ")" + modifier + ").getOrElse {")
		buffer.append(System.lineSeparator)
		buffer.append("        new RuntimeException(\"TIME: Could not parse input \" + str + \" as a Date\")")
		buffer.append(System.lineSeparator)
		buffer.append("      }")
		buffer.append(System.lineSeparator)
		buffer.append("    }")
		buffer.append(System.lineSeparator)
		buffer.append("}")
		buffer.append(System.lineSeparator)
		new Parameter(varname, Types.Int)		
	}
	
	/**
	 * generates implementations for built in transformation functions
	 */
	private def Parameter generateBuiltinColumnFunction(BuiltinColumnFunction function, List<Parameter> paramVars, StringBuffer buffer, GeneratorState state, HeaderInformation header, MaterializedViewStatement view, List<BodyStatement> bodyStatements) {
		val newVar = "functionResult" + state.nextVarNumberAndIncrement
		switch function.name {
			case BuiltinColumnFunctions.CASTDATE: { 
				if(paramVars.get(0).vartype != Types.Any && paramVars.get(0).vartype != Types.Date && paramVars.get(0).vartype != Types.Long && paramVars.get(0).vartype != Types.Int && paramVars.get(0).vartype != Types.String) {
					warnings.add("Type " + paramVars.get(0).vartype + " not expected for CASTDATE. Assuming String.")
				}
				// convert input to date; we assume Any as input
				buffer.append("val " + newVar + " = " + paramVars.get(0).varname + " match {")
				buffer.append(System.lineSeparator)
				buffer.append("  case d: Date => " + HesseHadoopLibraryGenerator::getLib(header, view) + ".dateLib(d.getTime)")
				buffer.append(System.lineSeparator)
				// assume the long to be a Unix-Epoch time; clobber it to match the day
				buffer.append("  case l: Long => " + HesseHadoopLibraryGenerator::getLib(header, view) + ".dateLib(l)")
				buffer.append(System.lineSeparator)
				// assume the int to be an old Unix timestamp in seconds
				buffer.append("  case i: Int => " + HesseHadoopLibraryGenerator::getLib(header, view) + ".dateLib(i.toLong * 1000L)")
				buffer.append(System.lineSeparator)
				// default is to assume its a string; this is a difficult matter...
				// is it a Number/Long? -> Unix-Epoch time
				// is it of format "YYYY-MM-DD..."? -> make a string date
				buffer.append("  case s => val str = s.toString")
				buffer.append(System.lineSeparator)
				buffer.append("    Try(" + HesseHadoopLibraryGenerator::getLib(header, view) + ".dateLib(str.toLong)).getOrElse {")
				buffer.append(System.lineSeparator)
				buffer.append("      Try(" + HesseHadoopLibraryGenerator::getLib(header, view) + ".dateLib(" + HesseHadoopLibraryGenerator::getLib(header, view) + ".stringToDate(str))).getOrElse {")
				buffer.append(System.lineSeparator)
				buffer.append("        throw new RuntimeException(\"DATE: Could not parse input \" + str + \" as a Date\")")
				buffer.append(System.lineSeparator)
				buffer.append("      }")
				buffer.append(System.lineSeparator)
				buffer.append("    }")
				buffer.append(System.lineSeparator)
				buffer.append("}")
				buffer.append(System.lineSeparator)
				new Parameter(newVar, Types.Date)
			}
			case BuiltinColumnFunctions.MAKEDATE: {
				// all params must be int, long, double, float, BigInt, BigDecimal or a string that can be converted to int...
				buffer.append("val " + newVar + " = " + HesseHadoopLibraryGenerator::getLib(header, view) + ".makeDate(" +  
					HesseHadoopLibraryGenerator::getLib(header, view) + ".castToInt(" + paramVars.get(0).varname + "), " + 
					HesseHadoopLibraryGenerator::getLib(header, view) + ".castToInt(" + paramVars.get(1).varname + "), " + 
					HesseHadoopLibraryGenerator::getLib(header, view) + ".castToInt(" + paramVars.get(2).varname + "), 0, 0, 0)")
				buffer.append(System.lineSeparator)
				new Parameter(newVar, Types.Date)
			}
			case BuiltinColumnFunctions.CASTTIME: {
				if(paramVars.get(0).vartype != Types.Any && paramVars.get(0).vartype != Types.Date && paramVars.get(0).vartype != Types.Long && paramVars.get(0).vartype != Types.Int && paramVars.get(0).vartype != Types.String) {
					warnings.add("Type " + paramVars.get(0).vartype + " not expected for CASTTIME. Assuming String.")
				}
				// convert input to date; we assume Any as input
				buffer.append("val " + newVar + " = " + paramVars.get(0).varname + " match {")
				buffer.append(System.lineSeparator)
				// assume the long to be a Unix-Epoch time
				buffer.append("  case l: Long => new Date(l)")
				buffer.append(System.lineSeparator)
				// assume the int to be an old Unix timestamp in seconds
				buffer.append("  case i: Int => new Date(i.toLong * 1000L)")
				buffer.append(System.lineSeparator)
				// default is to assume its a string; this is a difficult matter...
				// is it a Number/Long? -> Unix-Epoch time
				// is it of format "YYYY-MM-DD..."? -> make a string date
				buffer.append("  case s => val str = s.toString")
				buffer.append(System.lineSeparator)
				buffer.append("    Try(new Date(str.toLong)).getOrElse {")
				buffer.append(System.lineSeparator)
				buffer.append("      Try(" + HesseHadoopLibraryGenerator::getLib(header, view) + ".stringToDate(str)).getOrElse {")
				buffer.append(System.lineSeparator)
				buffer.append("        throw new RuntimeException(\"TIME: Could not parse input \" + str + \" as a Date\")")
				buffer.append(System.lineSeparator)
				buffer.append("      }")
				buffer.append(System.lineSeparator)
				buffer.append("    }")
				buffer.append(System.lineSeparator)
				buffer.append("}")
				buffer.append(System.lineSeparator)
				new Parameter(newVar, Types.Date)		
			}
			case BuiltinColumnFunctions.MAKETIME: {
				buffer.append("val " + newVar + " = " + HesseHadoopLibraryGenerator::getLib(header, view) + ".makeDate(" +  
					HesseHadoopLibraryGenerator::getLib(header, view) + ".castToInt(" + paramVars.get(0).varname + "), " + 
					HesseHadoopLibraryGenerator::getLib(header, view) + ".castToInt(" + paramVars.get(1).varname + "), " + 
					HesseHadoopLibraryGenerator::getLib(header, view) + ".castToInt(" + paramVars.get(2).varname + "), " +				
					HesseHadoopLibraryGenerator::getLib(header, view) + ".castToInt(" + paramVars.get(3).varname + "), " + 
					HesseHadoopLibraryGenerator::getLib(header, view) + ".castToInt(" + paramVars.get(4).varname + "), " + 
					HesseHadoopLibraryGenerator::getLib(header, view) + ".castToInt(" + paramVars.get(5).varname + "))") 
				buffer.append(System.lineSeparator)
				new Parameter(newVar, Types.Date)		
			}
			case BuiltinColumnFunctions.MINUTE: generateTimeGetter(newVar, Calendar.MINUTE, paramVars, "", buffer, header, view)
			case BuiltinColumnFunctions.SECOND: generateTimeGetter(newVar, Calendar.SECOND, paramVars, "", buffer, header, view)
			case BuiltinColumnFunctions.HOUR: generateTimeGetter(newVar, Calendar.HOUR, paramVars, "", buffer, header, view)
			case BuiltinColumnFunctions.MILLISECOND:	generateTimeGetter(newVar, Calendar.MILLISECOND, paramVars, "", buffer, header, view)
			case BuiltinColumnFunctions.DAY: generateTimeGetter(newVar, Calendar.DAY_OF_MONTH, paramVars, "", buffer, header, view)
			case BuiltinColumnFunctions.MONTH: generateTimeGetter(newVar, Calendar.MONTH, paramVars, " + 1", buffer, header, view)
			case BuiltinColumnFunctions.YEAR: generateTimeGetter(newVar, Calendar.YEAR, paramVars, "", buffer, header, view)
			case BuiltinColumnFunctions.MD5: {
				if(paramVars.get(0).vartype != Types.Any && paramVars.get(0).vartype != Types.String && paramVars.get(0).vartype != Types.ByteArray) {
					warnings.add("Type " + paramVars.get(0).vartype + " not expected for MD5. Converting to String.")
				}
				buffer.append("val " + newVar + " = " + paramVars.get(0).varname + " match {")
				buffer.append(System.lineSeparator)
				buffer.append("  case a: Array[Byte] => MessageDigest.getInstance(\"MD5\").digest(a)")
				buffer.append(System.lineSeparator)
				buffer.append("  case s => MessageDigest.getInstance(\"MD5\").digest(s.toString.getBytes(\"UTF-8\"))")
				buffer.append(System.lineSeparator)
				buffer.append("}")
				buffer.append(System.lineSeparator)
				new Parameter(newVar, Types.Int)
			}
			case BuiltinColumnFunctions.CRC32: {
				if(paramVars.get(0).vartype != Types.Any && paramVars.get(0).vartype != Types.String && paramVars.get(0).vartype != Types.ByteArray) {
					warnings.add("Type " + paramVars.get(0).vartype + " not expected for CRC32. Converting to String.")
				}
				buffer.append("val " + newVar + " = " + paramVars.get(0).varname + " match {")
				buffer.append(System.lineSeparator)
				buffer.append("  case a: Array[Byte] =>") 
				buffer.append(System.lineSeparator)
				buffer.append("    val checksum = new CRC32()")
				buffer.append(System.lineSeparator)
				buffer.append("    checksum.update(a, 0, a.length)")
				buffer.append(System.lineSeparator)
				buffer.append("    checksum.getValue()")
				buffer.append(System.lineSeparator)
				buffer.append("  case s =>") 
				buffer.append(System.lineSeparator)
				buffer.append("    val checksum = new CRC32()")
				buffer.append(System.lineSeparator)
				buffer.append("    checksum.update(s.toString.getBytes(\"UTF-8\"), 0, s.toString.getBytes(\"UTF-8\").length)")
				buffer.append(System.lineSeparator)
				buffer.append("    checksum.getValue()")
				buffer.append(System.lineSeparator)
				buffer.append("}")
				buffer.append(System.lineSeparator)
				new Parameter(newVar, Types.ByteArray)
			}
			case BuiltinColumnFunctions.ABS: {
				buffer.append("val " + newVar + " = " + paramVars.get(0).varname + " match {")
				buffer.append(System.lineSeparator)
				buffer.append("  case i: Int => scala.math.abs(i)")
				buffer.append(System.lineSeparator)
				buffer.append("  case d: Double => scala.math.abs(d)")
				buffer.append(System.lineSeparator)
				buffer.append("  case l: Long => scala.math.abs(l)")
				buffer.append(System.lineSeparator)
				buffer.append("  case f: Float => scala.math.abs(f)")
				buffer.append(System.lineSeparator)
				buffer.append("  case b: BigInt => b.abs")
				buffer.append(System.lineSeparator)
				buffer.append("  case b: BigDecimal => b.abs")
				buffer.append(System.lineSeparator)
				buffer.append("  case b: JBigInteger => b.abs")
				buffer.append(System.lineSeparator)
				buffer.append("  case b: JBigDecimal => b.abs")
				buffer.append(System.lineSeparator)
				buffer.append("  case s => s.toString.abs")
				buffer.append(System.lineSeparator)
				buffer.append("}")
				buffer.append(System.lineSeparator)
				new Parameter(newVar, Types.Any)
			}
			case BuiltinColumnFunctions.MID: {
				if(paramVars.get(0).vartype != Types.Any && paramVars.get(0).vartype != Types.String) {
					warnings.add("Type " + paramVars.get(0).vartype + " should be String for MID. Converting to String.")
				}
				buffer.append("val " + newVar + " = " +  paramVars.get(0).varname + ".substring(" + 
					HesseHadoopLibraryGenerator::getLib(header, view) + ".castToInt(" + paramVars.get(1).varname + "), " + 
					HesseHadoopLibraryGenerator::getLib(header, view) + ".castToInt(" + paramVars.get(2).varname + "))")
				buffer.append(System.lineSeparator)
				new Parameter(newVar, Types.String)
			}
			case BuiltinColumnFunctions.CASTINT: {
				buffer.append("val " + newVar + " = " + HesseHadoopLibraryGenerator::getLib(header, view) + ".castToInt("+  paramVars.get(0).varname + ")")
				buffer.append(System.lineSeparator)
				new Parameter(newVar, Types.Int)
			}
			case BuiltinColumnFunctions.CASTLONG: {
				buffer.append("val " + newVar + " = " + HesseHadoopLibraryGenerator::getLib(header, view) + ".castToLong("+  paramVars.get(0).varname + ")")
				buffer.append(System.lineSeparator)
				new Parameter(newVar, Types.Long)				
			}	
			case BuiltinColumnFunctions.CASTDOUBLE: {
				buffer.append("val " + newVar + " = " + HesseHadoopLibraryGenerator::getLib(header, view) + ".castToDouble("+  paramVars.get(0).varname + ")")
				buffer.append(System.lineSeparator)
				new Parameter(newVar, Types.Double)								
			}
			case BuiltinColumnFunctions.CASTFLOAT: {
				buffer.append("val " + newVar + " = " + HesseHadoopLibraryGenerator::getLib(header, view) + ".castToFloat("+  paramVars.get(0).varname + ")")
				buffer.append(System.lineSeparator)
				new Parameter(newVar, Types.Float)												
			}				
			case BuiltinColumnFunctions.CASTSTRING: {
				buffer.append("val " + newVar + " = " + paramVars.get(0).varname + ".toString")
				buffer.append(System.lineSeparator)
				new Parameter(newVar, Types.String)
			}
			case BuiltinColumnFunctions.LENGTH: {
				if(paramVars.get(0).vartype != Types.Any && paramVars.get(0).vartype != Types.String) {
					warnings.add("Type " + paramVars.get(0).vartype + " should be String for LENGTH. Converting to String.")
				}				
				buffer.append("val " + newVar + " = " + paramVars.get(0).varname + ".toString.length")
				buffer.append(System.lineSeparator)
				new Parameter(newVar, Types.Int)
			}
			default: throw new GeneratorError("Cannot generate code for unknown function " + function.name)
		}
	}
}
