package utilities;

import java.util.Arrays;

/**
 * This is the class file for a Tuple from a relation.
 * @author E K
 *
 */
public class Tuple {
	
	public int[] vals;
			
	public Tuple(byte[] tupleAsBytes, int numCols, int tupleSize) {
		vals 			= new int[numCols];
		int byteIndex 	= 0;
		for (int i = 0; i < numCols; i++) {
			vals[i] = tupleAsBytes[byteIndex+3] & 0xFF | (tupleAsBytes[byteIndex+2] & 0xFF) << 8 |
					(tupleAsBytes[byteIndex+1] & 0xFF) << 16 | (tupleAsBytes[byteIndex] & 0xFF) << 24;
			byteIndex += 4;
		}
	}
	public Tuple(String poisonPill) {
		vals = new int[13];
		for (int i = 0; i < 13; i++) {
			if (poisonPill.equals("poisonPill")) {
				vals[i] = -999999921;
			} else {
				vals[i] = -999899948;
			}
		}
	}
	//overload constructor to concat tuples 
	public Tuple(Tuple t1, Tuple t2) {
		vals 	= new int[t1.vals.length + t2.vals.length];
		int i 	= 0;
		int j 	= 0;
		while (i < vals.length) {
			if (i < t1.vals.length) {
				vals[i] = t1.vals[i];
			} else {
				vals[i] = t2.vals[j];
				j++;
			}
			i++;
		}
	}
	public boolean checkBigPoisonPill() {
		for (int i : vals) {
			if (i != -999999921) {
				return false;
			}
		}
		return true;
	}
	public boolean checkSmallPoisonPill() {
		for (int i : vals) {
			if (i != -999899948) {
				return false;
			}
		}
		return true;
	}
	public int[] getRowInts() {
		return vals;
	}
	public int getValue(int col) {
		return vals[col];
	}
	
	public String toString() {
		return Arrays.toString(vals);
	}
}
