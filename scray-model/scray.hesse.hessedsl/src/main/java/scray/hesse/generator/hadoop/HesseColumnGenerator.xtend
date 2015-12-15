package scray.hesse.generator.hadoop

import java.math.BigDecimal
import scray.hesse.generator.GeneratorError
import scray.hesse.generator.GeneratorState
import scray.hesse.generator.hadoop.HesseFunctionGenerator.Parameter
import scray.hesse.generator.hadoop.HesseFunctionGenerator.Types
import scray.hesse.hesseDSL.ConstantColumn
import scray.hesse.hesseDSL.ConstantDoubleColumnvalue
import scray.hesse.hesseDSL.ConstantIntColumn
import scray.hesse.hesseDSL.ConstantLongValue
import scray.hesse.hesseDSL.ConstantStringColumn
import scray.hesse.hesseDSL.SelectColumn
import scray.hesse.generator.HeaderInformation
import scray.hesse.hesseDSL.MaterializedViewStatement

/**
 * generate column specifications
 */
class HesseColumnGenerator {
	
	/**
	 * generates code that produces a basic literal value
	 */
	static def Parameter generateConstantColumnSpecification(ConstantColumn col) {
		switch(col) {
			ConstantIntColumn: {
				new Parameter(col.value.toString, Types.Int)
			}
			ConstantStringColumn: {
				new Parameter('''"""''' + col.value + '''"""''', Types.String)
			}
			ConstantDoubleColumnvalue: {
				val bigValue = new BigDecimal(col.value)
				new Parameter(bigValue.toString + "D", Types.Double)
			}
			ConstantLongValue: {
				new Parameter(col.value.toString + "L", Types.Long)
			}
			default: {
				throw new GeneratorError("Constant/Literal column has an unknown type: " + col.getClass.getName)
			}
		}
	}
	
	/**
	 * generates code that pulls a column out from the row
	 * TODO: allow and check for table references 
	 */
	static def Parameter generateRowColumnSpecification(SelectColumn col, StringBuffer buffer, GeneratorState state, HeaderInformation header, MaterializedViewStatement view) {
		val varname = "column" + state.nextVarNumberAndIncrement
		buffer.append("val ")
		buffer.append(varname)
		buffer.append(" = ")
		buffer.append(HesseHadoopLibraryGenerator::getLib(header, view))
		buffer.append(".getAnyObject(value, \"\"\"")
		buffer.append(col.name)
		buffer.append("\"\"\")")
		buffer.append(System.lineSeparator)
		new Parameter(varname, Types.Any)
	}	
}