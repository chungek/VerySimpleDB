//This class contains the functions that build RA operators and conntec them to form a tree
package execution;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.Map.Entry;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicIntegerArray;
import java.util.concurrent.atomic.AtomicLongArray;

import concurrentRA.ConcurrentColScan;
import concurrentRA.ConcurrentFilter;
import concurrentRA.ConcurrentJoin;
import concurrentRA.ConcurrentOperator;
import concurrentRA.ConcurrentScan;
import concurrentRA.ConcurrentSum;
import utilities.Catalog;
import utilities.FilterPredicate;
import utilities.JoinPredicate;

public class Optimizer {
	public static Pipeline buildPipeline(Query q, ConcurrentHashMap<String, Catalog> catalogs) throws IOException {
		StringBuilder sb = new StringBuilder();
		String relNames = "";
		for (String rel : q.relationsNeeded) {
			sb.append(rel);
		}
		relNames = sb.toString();
		HashMap<String, Long> possibleOrders = new HashMap<String, Long>();
 		//get the min cost order
		HashMap<String, ArrayList<Integer>> reqCols = new HashMap<String, ArrayList<Integer>>();
		getRequiredProjections(q, reqCols);
		String bestJoinOrder = findBestJoinOrder(q.relationsNeeded, q.joinPreds, catalogs);
//		System.out.println(bestJoinOrder);
		//now we start building the pipeline based on the "optimal" ordering!
		LinkedList<Thread> entryAndExit       = new LinkedList<Thread>();
		LinkedList<Thread> rightChildScans 		= new LinkedList<Thread>();
		LinkedList<Thread> joinsAndRightFilts = new LinkedList<Thread>();
		ArrayList<String> joinsSoFar = new ArrayList<String>();
		ArrayList<String> jPreds 		 = new ArrayList<String>(Arrays.asList(q.joinPreds));
		ArrayList<String> fPreds 		 = new ArrayList<String>(Arrays.asList(q.filterPreds));
		//set up left-most scan
		String firstScan = bestJoinOrder.charAt(0) + "";
		ConcurrentOperator prev, leftChildOfJoin;
		ArrayList<Integer> tempCols = reqCols.get(firstScan);
		String[] cols = new String[tempCols.size()];
		for (int i = 0; i < tempCols.size(); i++) {
			cols[i] = tempCols.get(i)+"";
		}
		ConcurrentColScan entryScan = new ConcurrentColScan(catalogs.get(firstScan), true, cols);
		joinsSoFar.add(firstScan);
		prev = entryScan;
		leftChildOfJoin = entryScan;
//		allThreads.add(new Thread(entryScan));
		entryAndExit.add(new Thread (entryScan));
		String[] fPredsScan = findFilterPreds(fPreds, firstScan.charAt(0));
		if (fPredsScan.length != 0) {
			ConcurrentFilter f = new ConcurrentFilter(fPredsScan, entryScan);
			prev = f;
			leftChildOfJoin = f;
			entryAndExit.add(new Thread(f));
		}
		//loop over subsequent joins
		for (int i = 1; i < bestJoinOrder.length(); i++) {
			//get the right child of each join
			char nextJoinRel = bestJoinOrder.charAt(i);
			joinsSoFar.add(nextJoinRel+"");
			tempCols = reqCols.get(nextJoinRel+"");
			cols = new String[tempCols.size()];
			for (int j = 0; j < tempCols.size(); j++) {
				cols[j] = tempCols.get(j)+"";
			}
			ConcurrentColScan rightChildScan = new ConcurrentColScan(catalogs.get(nextJoinRel+""), false, cols);
			rightChildScans.add(new Thread(rightChildScan));
			prev = rightChildScan;
			String[] nextFiltPreds = findFilterPreds(fPreds, nextJoinRel);
			//check if filter present
			if (nextFiltPreds.length != 0) {
				ConcurrentFilter nextFilt = new ConcurrentFilter(nextFiltPreds, (ConcurrentColScan) prev);
				joinsAndRightFilts.add(new Thread(nextFilt));
				prev = nextFilt;
			}
			String[] nextJPredStrings = findJoinPreds(jPreds, joinsSoFar);
			JoinPredicate[] nextJoinPreds = new JoinPredicate[nextJPredStrings.length];
			for (int j = 0; j < nextJoinPreds.length; j++) {
				nextJoinPreds[j] = new JoinPredicate(leftChildOfJoin.getTupleInfo(), prev.getTupleInfo(), nextJPredStrings[j]);
			}
			ConcurrentJoin currJoin = new ConcurrentJoin(leftChildOfJoin, prev, nextJoinPreds);
			leftChildOfJoin = currJoin;
			joinsAndRightFilts.add(new Thread(currJoin));
		}
		//connect the sum operator after joins are all connected
		AtomicLongArray sums = new AtomicLongArray(q.sumsNeeded.length + 1);
		ConcurrentSum sum = new ConcurrentSum(leftChildOfJoin, q.sumsNeeded, sums);
//		System.out.println(Arrays.toString(sum.sumIndices));
		entryAndExit.add(new Thread(sum));
		Pipeline pipe = new Pipeline(entryAndExit, joinsAndRightFilts, rightChildScans, sums);
		return pipe;
	}

	public static void getRequiredProjections(Query q, HashMap<String, ArrayList<Integer>> reqCols) {
		String tempRel;
		int col;
		ArrayList<Integer> temp;
		for (String s : q.sumsNeeded) {
			temp = new ArrayList<Integer>(); //{A.c23}
			String[] split = s.replace("SUM(", "").replace(")", "").split(".c"); //{"A", "23)"}
			tempRel = split[0];
			col = Integer.parseInt(split[1]);
			temp = reqCols.get(tempRel);
			if (temp == null) {
				temp = new ArrayList<Integer>();
			}
			temp.add(col);
			reqCols.put(tempRel, temp);
		}
		for (String j : q.joinPreds) {
			String[] split = j.split(" = "); //A.c2 , C.c0
			for (String curr : split) {
				temp = new ArrayList<Integer>();
				String[] split2 = curr.split(".c"); // A , 2
				tempRel = split2[0];
				col = Integer.parseInt(split2[1]);
				if (!reqCols.containsKey(tempRel)) {
					temp.add(col);
					reqCols.put(tempRel, temp);
				} else {
					temp = reqCols.get(tempRel);
					if (!temp.contains(col)) {
						temp.add(col);
						Collections.sort(temp);
						reqCols.put(tempRel, temp);
					}
				}
			}
		}
		for (String f : q.filterPreds) {
			temp = new ArrayList<Integer>();
			String[] split = f.split(" "); // A.c46 , < , -2101
			tempRel = split[0].substring(0, 1);
			col = Integer.parseInt(split[0].split("c")[1]);
			if (!reqCols.containsKey(tempRel)) {
				temp.add(col);
				reqCols.put(tempRel, temp);
			} else {
				temp = reqCols.get(tempRel);
				if (!temp.contains(col)) {
					temp.add(col);
					Collections.sort(temp);
					reqCols.put(tempRel, temp);
				}
			}
		}
	}
	//finds and removes preds for a scan
	public static String[] findFilterPreds(ArrayList<String> fPreds, char scan) {
		ArrayList<String> toRemove = new ArrayList<String>();
		for (String s : fPreds) {
			if (s.charAt(0) == scan) {
				toRemove.add(s);
			}
		}
		for (String rem : toRemove) {
			fPreds.remove(rem);
		}
		String[] validPreds = new String[toRemove.size()];
		int count = 0;
		for (String s : toRemove) {
			validPreds[count] = s;
			count++;
		}
		return validPreds;
	}
	//[A.c2 = C.c0, A.c5 = F.c0, A.c9 = J.c0, A.c6 = G.c0, A.c7 = H.c0, A.c3 = D.c0]
	public static String[] findJoinPreds(ArrayList<String> jPreds, ArrayList<String> joinsSoFar) {
		ArrayList<String> preds = new ArrayList<String>();
		ArrayList<String> toRemove = new ArrayList<String>();
		char outerRel = joinsSoFar.get(joinsSoFar.size() - 1).charAt(0);
		//loop over jpreds to look for relevant ones
		for (String pred : jPreds) {
			String[] currPred = pred.split(" = ");
			//check if outer relation exists in the pred
			if (currPred[0].charAt(0) == outerRel || currPred[1].charAt(0) == outerRel) {
				//outRel exists in this pred, now we check that both rels in pred are in curr join order
				boolean foundInner = false, foundOuter = false;
				for (int j = 0; j < joinsSoFar.size(); j++) {
					if ((currPred[0].charAt(0)+"").equals(joinsSoFar.get(j)) ) {
						foundInner = true;
						break;
					}
				}
				for (int h = 0; h < joinsSoFar.size(); h++) {
					if ((currPred[1].charAt(0)+"").equals(joinsSoFar.get(h))) {
						foundOuter = true;
						break;
					}
				}
				//predicate is valid
				if (foundInner && foundOuter) {
					toRemove.add(pred);
				}
			}
		}
		//remove valid preds from original predlist
		for (String rem : toRemove) {
			jPreds.remove(rem);
		}
		//add flip method
		Object[] temp = toRemove.toArray();
		String[] validPreds = new String[toRemove.size()];
		int count = 0;
		for (Object o : temp) {
			String currPred = (String) o;
			String[] currTemp = currPred.split(" = ");
			if (currTemp[1].charAt(0) != outerRel) {
				currPred = flipPred(currPred);
			}
			validPreds[count] = currPred;
			count++;
		}
		return validPreds;
	}
	//flips the oder of the predicate to match the join order
	public static String flipPred(String pred) {
		String[] temp = pred.split(" = ");
		StringBuilder sb = new StringBuilder();
		sb.append(temp[1]);
		sb.append(" = ");
		sb.append(temp[0]);
		return sb.toString();
	}

	public static LinkedList<Thread> setUpJoin(Query q, String order) {
		LinkedList<Thread> joinNodes = new LinkedList<Thread>();

		return joinNodes;
	}
	//sorts the tables by num rows it has
	public static ArrayList<String> sort(ArrayList<String> sortedTables, int numRels, String[] relsNeeded, HashMap<String, Integer> relRows) {
		ArrayList<String> tempRels = new ArrayList<String>(Arrays.asList(relsNeeded));
		for (int i = 0; i < numRels; i++) {
			int max = Integer.MIN_VALUE;
			String currMax = "";
			for (String rel : tempRels) {
				if (relRows.get(rel) > max) {
					max = relRows.get(rel);
					currMax = rel;
				}
			}
			sortedTables.add(currMax);
			tempRels.remove(currMax);
		}
		return sortedTables;
	}
	//tries to make big relations as close to the left most child as possible
	public static String findBestJoinOrder(String[] relsNeeded, String[] jPreds, ConcurrentHashMap<String, Catalog> catalogs) {
		String best = "";
		int numRels = relsNeeded.length;
		String[] preds = readPreds(jPreds);
		//make reference for num of rows in each table
		HashMap<String, Integer> relRows = new HashMap<String, Integer>();
		for (String rel : relsNeeded) {
			relRows.put(rel, (Integer) catalogs.get(rel).getNumRows());
		}
		//sort the relations by num rows
		ArrayList<String> sortedTables = new ArrayList<String>();
		sort(sortedTables, numRels, relsNeeded, relRows);
		ArrayList<String> tempPreds = new ArrayList<String>(Arrays.asList(preds));
		ArrayList<String> joinOrderSoFar = new ArrayList<String>();
		best = findEntryJoin(tempPreds, relRows, sortedTables);
		while (sortedTables.size() > 0) {
			String toRemove = "";
			//try to find next join from sorted tables
			for (String table : sortedTables) {
				boolean foundPred = false;
				//loop thru all preds to see if this option is valid
				for (String pred : preds) {
					if (pred.charAt(0) == table.charAt(0)) {
						if (best.indexOf(pred.charAt(1)) >= 0) {
							foundPred = true;
							break;
						}
					} else if (pred.charAt(1) == table.charAt(0)) {
						if (best.indexOf(pred.charAt(0)) >= 0) {
							foundPred = true;
							break;
						}
					}
				}
				if (foundPred) {
					best += table;
					toRemove = table;
					break;
				}
			}
			sortedTables.remove(toRemove);
		}
		return best;
	}
	//find the join with the largest entry join with largest left child
	public static String findEntryJoin(ArrayList<String> tempPreds, HashMap<String, Integer> relRows, ArrayList<String> sortedTables) {
		int max = Integer.MIN_VALUE;
		int babymax = Integer.MIN_VALUE;
		String currLeftMax = "";
		String currRightMax = "";
		for (String pred : tempPreds) {
			//case where left side of pred has more rows and left side has more than 2nd max
			if (relRows.get(pred.charAt(0)+"") >= max && relRows.get(pred.charAt(1)+"") > babymax) {
				currLeftMax = pred.charAt(0)+"";
				currRightMax = pred.charAt(1)+"";
				max = relRows.get(pred.charAt(0)+"");
				babymax = relRows.get(pred.charAt(1)+"");
			//case where right side of pred has more rows and left side has more than 2nd max
			} else if (relRows.get(pred.charAt(1)+"") >= max && relRows.get(pred.charAt(0)+"") > babymax) {
				currLeftMax = pred.charAt(1)+"";
				currRightMax = pred.charAt(0)+"";
				max = relRows.get(pred.charAt(1)+"");
				babymax = relRows.get(pred.charAt(0)+"");
			}
		}
		sortedTables.remove(currLeftMax+"");
		sortedTables.remove(currRightMax+"");
		return (currLeftMax + "" + currRightMax);
	}
	//returns array of joinPreds w just their relation names
	public static String[] readPreds(String[] joinPreds) {
		String[] preds = new String[joinPreds.length];
		for (int i = 0; i < joinPreds.length; i++) {
			String[] splitPreds = joinPreds[i].split(" = ");
			preds[i] = splitPreds[0].charAt(0) + "" + splitPreds[1].charAt(0);
		}
		return preds;
	}

	public static boolean shouldSwap(char str[], int start, int curr) {
        for (int i = start; i < curr; i++) {
            if (str[i] == str[curr]) {
                return false;
            }
        }
        return true;
    }
}
