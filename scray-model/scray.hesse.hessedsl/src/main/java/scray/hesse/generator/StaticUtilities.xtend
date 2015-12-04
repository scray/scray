package scray.hesse.generator

import java.util.HashSet
import java.util.List
import java.util.Set
import org.eclipse.emf.ecore.EObject
import scray.hesse.hesseDSL.BodyStatement
import scray.hesse.hesseDSL.DBMSRefTable
import scray.hesse.hesseDSL.MaterializedViewStatement
import scray.hesse.hesseDSL.PrimitiveTable
import scray.hesse.hesseDSL.SelectColumn
import scray.hesse.hesseDSL.SelectStatement
import scray.hesse.hesseDSL.SubSelectingTable
import scray.hesse.hesseDSL.SupportedDBMSSystems
import scray.hesse.hesseDSL.ViewStatement

class StaticUtilities {
	/**
     * simple utility method to check whether an EObject has a given property
     * with the name given
     */
    def static isSet(EObject o, String propertyName) {
        return o.eIsSet(o.eClass().getEStructuralFeature(propertyName));
    }


    /**
     * simply adds a platform dependent newline to the given StringBuilder
     */
    def static nl(StringBuilder builder) {
        builder.append(System::getProperty("line.separator"))
    }
    
    /**
     * container for dbms and dbid
     */
    public static class DBMSAndDBID {
    	new(SupportedDBMSSystems dbms, String dbid) {
    		this.dbid = dbid
    		this.dbms = dbms
    	}
    	public val String dbid
    	public val SupportedDBMSSystems dbms
    } 
    
    /**
     * return dbms and dbid for a given materialized view 
     */
    def static DBMSAndDBID getDBMSAndDBIdforStatement(MaterializedViewStatement statement, HeaderInformation header) {
    	if(isSet(statement, "dbms")) {
    		
    	} else {
    		
    	}
    }
    
    /**
     * recursive function
     */
    private static def void bodyStatementExpansion(SelectStatement select, HeaderInformation header, SupportedDBMSSystems dbms, Set<String> result) {
		if(select.columns.allColumns && header.defaultdbsystem == dbms && header.dbid != null) {
			result.add(header.dbid)
		}
		if(isSet(select.columns, "columns") && select.columns.columns.size > 0) {
			select.columns.columns.forEach [ coldef |
				if(coldef instanceof SelectColumn) {
					// TODO: add support for sub-selects on columns
				}
			]
		}
		select.tables.tables.forEach [ tabledef |
			if(tabledef instanceof PrimitiveTable) {
				if(dbms == header.defaultdbsystem && header.dbid != null) {
					result.add(header.dbid)
				}
			} else if(tabledef instanceof DBMSRefTable) {
				val table = (tabledef as DBMSRefTable)
				if(table.dbms == dbms) {
					result.add(table.dbid)
				}
			} else if(tabledef instanceof SubSelectingTable) {
				bodyStatementExpansion(tabledef.select, header, dbms, result)
			}
		]
    }
    
    /**
     * returns all db-ids for a given dbms configuration that are used as target or sources
     * TODO: add support for sub-selects if necessary
     */
    def static getAllDBIds(HeaderInformation header, List<BodyStatement> bodyStatements, SupportedDBMSSystems dbms, boolean source, boolean target) {
    	val Set<String> result = new HashSet<String>()
    	if(target) {
    		// if there are any materialized views which do have our dbms set we return it
    		bodyStatements.forEach[statement | 
    			if(statement instanceof MaterializedViewStatement) {
    				val stmt = (statement as MaterializedViewStatement)
    				if(isSet(stmt, "dbms") && stmt.dbms == dbms && isSet(stmt, "dbid")) {
    					result.add(stmt.dbid)
    				} else {
    					if(dbms == header.defaultdbsystem && header.dbid != null) {
    						result.add(header.dbid)
    					}
    				}
    			}
    		] 
    	}
    	if(source) {
			// if there are any referenced tables which do have our dbms set we return it
			bodyStatements.forEach[statement | 
				if(statement instanceof MaterializedViewStatement || statement instanceof ViewStatement) {
					bodyStatementExpansion(statement.select, header, dbms, result)
    			}
    		]
    	}
    	result
    }
    
    /**
     * creates a variable declaration on the buffer
     */
    def static String createVarDeclaration(StringBuffer buffer, String prefix, GeneratorState state) {
    	val varname = prefix + state.nextVarNumberAndIncrement
    	buffer.append("val ")
		buffer.append(varname)
		buffer.append(": Boolean = ")
		varname
    }
}