package utilities;

import java.util.ArrayList;
import java.util.Iterator;
/**
 * This the class file that contains all the static utility methods that components
 * of the system will need to use.
 * @author E K
 *
 */
public class Utils {
	
	public final int MAX_SIZE_BLOCK = 524288000;	/* 500mb of main mem dedicated to 1 block of resultant tuples */
	/**
	 * 
	 * @param namesOfChildren
	 * @param numColsOfChildren
	 * @param colInfo - in the form of B.c12
	 * @return
	 */
	public static int mapToTupleIndex(ArrayList<String> namesOfChildren, ArrayList<Integer> numColsOfChildren, String colInfo) {
		String tableName = colInfo.substring(0, 1);
		int offset = Integer.parseInt(colInfo.split("c")[1].replace(")", ""));
		int actualIndex = 0;
		Iterator<String> nameItr = namesOfChildren.iterator();
		Iterator<Integer> colItr = numColsOfChildren.iterator(); 
		String curr = "";
		boolean found = false;
		while (!found) {
			curr = nameItr.next();
			if (curr.equals(tableName)) {
				found = true;
			} else {
				actualIndex += colItr.next().intValue();
			}
		}
		actualIndex += offset;
		return actualIndex;
	}

	public static int map(TupleInfo t, String colInfo) {
		int actual = 0;
		String targetRel = colInfo.substring(0, 1);
		int target = Integer.parseInt(colInfo.split("c")[1].replace("(", ""));
		int offset = t.namesOfChildren.indexOf(targetRel);
		if (offset > 0) {
			for (int i = 0; i < offset; i++) {
				actual += t.numColsOfChildren.get(i);
			}
		}
		for (int i = 0; i < t.numColsOfChildren.get(t.namesOfChildren.indexOf(targetRel)); i++) {
			if (target == t.actualIndices.get(actual + i)) {
				actual += i;
				break;
			}
		}
		return actual;
	}
	
		
	public static int computeBlockSize() {
		int size = 0;
		return size;
	}
}
