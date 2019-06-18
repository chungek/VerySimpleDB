package utilities;

import java.util.List;
import java.util.ArrayList;

/**
 * This is the class file that stores the data of tuples that result from each operator
 * @author E K
 *
 */
public class TupleInfo {
	
	private int numCols;
	public ArrayList<String> namesOfChildren;
	public ArrayList<Integer> numColsOfChildren;
	public ArrayList<Integer> actualIndices;
	public TupleInfo(int numCols, String relationName) {
		 this.numCols	   	= numCols;
		 namesOfChildren 	= new ArrayList<String>();
		 numColsOfChildren 	= new ArrayList<Integer>();
		 namesOfChildren.add(relationName);
		 numColsOfChildren.add(numCols);
	}
	public TupleInfo(int numCols, String relationName, ArrayList<Integer> actualIndices) {
		 this.numCols	   	= numCols;
		 namesOfChildren 	= new ArrayList<String>();
		 numColsOfChildren 	= new ArrayList<Integer>();
		 namesOfChildren.add(relationName);
		 numColsOfChildren.add(numCols);
		 this.actualIndices = actualIndices;
	}
	/**
	 * overloads constructor for new tupleinfo on results of joins
	 */
	public TupleInfo(TupleInfo tupleInfo1, TupleInfo tupleInfo2) {
		numCols = tupleInfo1.getNumCols() + tupleInfo2.getNumCols();
		namesOfChildren = new ArrayList<String>();
		namesOfChildren.addAll(tupleInfo1.getNamesOfChildren());
		namesOfChildren.add(tupleInfo2.getNamesOfChildren().get(0));
		numColsOfChildren = new ArrayList<Integer>();
		numColsOfChildren.addAll(tupleInfo1.getNumColsOfChildren());
		numColsOfChildren.add(tupleInfo2.getNumCols());
		actualIndices = new ArrayList<Integer>();
		actualIndices.addAll(tupleInfo1.actualIndices);
		actualIndices.addAll(tupleInfo2.actualIndices);
	}
	
	public int getNumCols() {
		return numCols;
	}
	
	public ArrayList<String> getNamesOfChildren() {
		return namesOfChildren;
	}
	public ArrayList<Integer> getNumColsOfChildren() {
		return numColsOfChildren;
	}
	public String toString() {
		return "This tuple has " + numCols + " cols. " + namesOfChildren.toString() + " " + numColsOfChildren.toString() + " with actual indices " + actualIndices.toString();
	}
}

