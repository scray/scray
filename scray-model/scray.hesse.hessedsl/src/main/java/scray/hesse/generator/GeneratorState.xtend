package scray.hesse.generator

import java.util.HashMap
import org.eclipse.core.resources.IFile
import org.eclipse.core.resources.IWorkspaceRoot
import org.eclipse.core.resources.ResourcesPlugin
import org.eclipse.core.runtime.Path
import org.eclipse.emf.common.util.URI
import org.eclipse.xtext.builder.EclipseResourceFileSystemAccess2
import org.eclipse.xtext.generator.IFileSystemAccess
import scray.hesse.generator.hadoop.HesseHadoopGenerator
import scray.hesse.hesseDSL.DBMSRefTable
import scray.hesse.hesseDSL.PrimitiveTable
import scray.hesse.hesseDSL.RenamableColumnDefinition
import scray.hesse.hesseDSL.RenamableTableDefinition
import scray.hesse.hesseDSL.SelectStatement
import scray.hesse.hesseDSL.SubSelectingTable

/**
 * capsule for a single generator run
 */
class GeneratorState {

	/**
	 * factory method to create a new state object
	 */
	new(IFileSystemAccess fsa) {
		// initialize resources
		this.fsa = fsa as EclipseResourceFileSystemAccess2
		workspaceRoot = ResourcesPlugin::getWorkspace().getRoot()
		protectedRegions = new ProtectedRegions		
	}
	
	// protected regions must be globally accessible in this generator instance
	public val ProtectedRegions protectedRegions
	
	// used a reference for file locations
	public val IWorkspaceRoot workspaceRoot
	
	// ready to use filesystem object 
	public val EclipseResourceFileSystemAccess2 fsa
	
	// initialize various generators we will need to generate the code	
	public val hadoopGenerator = new HesseHadoopGenerator()
	
	// convenience lookup map for renamed columns
	public val variableNames = new HashMap<String, RenamableColumnDefinition>()
	
	// convenience lookup map for renamed tables
	public val tableNames = new HashMap<String, RenamableTableDefinition>()
	
	// used internally for naming variables
	var varnameposition = 0
	
	/**
	 * get variable and increment varposition
	 */
	def int getNextVarNumberAndIncrement() {
		val result = varnameposition
		varnameposition += 1
		result
	}
	
	/**
	 * reset the variable counter
	 */
	def void resetNextVarNumber() {
		varnameposition = 0
	}
	
	/**
	 * create an IFile from a given filename in the output directory 
	 */
	public def IFile getSystemFile(String filename) {
		var URI ur = fsa.getURI(filename)
        workspaceRoot.getFile(new Path(ur.toPlatformString(true)))
	}
	
	private def boolean setShortNamesSubSelect(SelectStatement select) {
		var boolean result = true
		val tables = select.tables.tables.map [ table |
			if(StaticUtilities::isSet(table, "alternateName")) {
				if(tableNames.get(table.alternateName) == null) {
					tableNames.put(table.alternateName, table)
					if(table.name instanceof SubSelectingTable) {
						setShortNamesSubSelect((table.name as SubSelectingTable).select)
					} else {
						true
					}
				} else {
					false
				}
			} else {
				if(table.name instanceof PrimitiveTable) {
					if(tableNames.get((table.name as PrimitiveTable).name) == null) {
						tableNames.put((table.name as PrimitiveTable).name, table)
					} else {
						false
					}
				} else if(table.name instanceof DBMSRefTable) {
					if(tableNames.get((table.name as DBMSRefTable).name) == null) {
						tableNames.put((table.name as DBMSRefTable).name, table)
					} else {
						false
					}					
				} else if(table.name instanceof SubSelectingTable) {
					setShortNamesSubSelect((table.name as SubSelectingTable).select)
				} else {
					false
				}
			}
		]
		if(tables.findFirst [ tresult | tresult == false ] != null) {
			result = false
		}
		result
	}
	
	/**
	 * scans the given view and intializes variables
	 */
	public def boolean setShortNames(SelectStatement select) {
		variableNames.clear
		tableNames.clear
		setShortNamesSubSelect(select)
	}
}