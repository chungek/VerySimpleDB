package utilities;

/**
 * This is the class file for a filter predicate
 * @author E K
 *
 */
public class FilterPredicate {
	
	public char operand;
	public int col, check;
	
	public FilterPredicate(String predicateAsString, TupleInfo tupleInfo) {
		String[] separated 	= predicateAsString.split(" ");
		operand 			= separated[1].charAt(0);
		col 				= Utils.map(tupleInfo, separated[0]);
		check 				= Integer.parseInt(separated[2]);
	}
	
	public boolean test(Tuple t) {
		if (operand == '<') {
			return t.getValue(col) < check;
		} else if (operand == '>') {
			return t.getValue(col) > check;
		} else {
			return t.getValue(col) == check;
		}
	}
	public String toString() {
		return col + " " + operand + " " + check;
	}
}
