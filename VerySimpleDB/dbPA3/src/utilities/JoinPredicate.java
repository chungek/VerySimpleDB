package utilities;
/**
 * This is the class file for a equijoin predicate
 * @author E K
 *
 */
public class JoinPredicate {
	
	public int index1, index2;
	
	/**
	 * 
	 * @param info1
	 * @param info2
	 * @param predStr - input the predicate as "A.c3 = B.c12"
	 */
	public JoinPredicate(TupleInfo info1, TupleInfo info2, String predStr) {
		String[] splitPred = predStr.split(" = ");
		index1 = Utils.map(info1, splitPred[0]);
		index2 = Utils.map(info2, splitPred[1]); 
	}
	
	public boolean test(Tuple t1, Tuple t2) {
		return (t1.getValue(index1) == t2.getValue(index2));
	}
	
	public String toString() {
		return "index1 = " + index1 + " index2 = " + index2;
	}
}