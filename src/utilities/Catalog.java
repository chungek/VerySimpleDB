package utilities;
/**
 * This is the class file of the catalog.
 * @author E K
 *
 */
public class Catalog {
	public String relationPath, relationName;				
	private int numCols, numRows;
	public boolean joined;
	public String[] relations;
	public int[] colIndex, numColsOfChildren;
	private TupleInfo tupleInfo;
	
	public Catalog(String relationPath, String relationName, int numCols, int numRows) {
		this.relationPath = relationPath;
		this.numCols	  = numCols;
		this.numRows 	  = numRows;
		this.relationName = relationName;
		colIndex 		  = new int[numCols];
		joined 			  = false;
		tupleInfo 		  = new TupleInfo(numCols, relationName);
		for (int i = 0; i < numCols; i++) {
			colIndex[i] = i;
		}
	}
	
	public String toString() {
		return "Relation " + relationName + " located at " + relationPath +
				" has " + numCols + " cols and " + numRows + " rows.";
	}
	/**
	 * method to get the .dat path	
	 * @return - the path of the relation on disk
	 */
	public String getRelationPath() {
		return relationPath;
	}
	/**
	 * method to get the num of cols
	 * @return - numCols
	 */
	public int getNumCols() {
		return numCols;
	}
	public TupleInfo getTupleInfo() {
		return tupleInfo;
	}
	/**
	 * method to get the num of rows
	 * @return - numRows
	 */
	public int getNumRows() {
		return numRows;
	}
	public boolean isJoined() {
		return joined;
	}
}
