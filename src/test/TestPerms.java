package test;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.Scanner;
import java.util.concurrent.ConcurrentHashMap;

import javax.swing.text.html.HTMLDocument.Iterator;

import execution.ConcurrentLoader;
import utilities.Catalog;

public class TestPerm {
	
	public static String[] readPreds(String[] joinPreds) {
		String[] preds = new String[joinPreds.length];
		for (int i = 0; i < joinPreds.length; i++) {
			String[] splitPreds = joinPreds[i].split(" = ");
			preds[i] = splitPreds[0].charAt(0) + "" + splitPreds[1].charAt(0);
		}
		return preds;
	}
	
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
	
	public static String findBestJoinOrder(String[] relsNeeded, String[] jPreds, ConcurrentHashMap<String, Catalog> catalogs) {
		String best = "";
		int numRels = relsNeeded.length;
		String[] preds = readPreds(jPreds);
		// make reference for num of rows in each table
		HashMap<String, Integer> relRows = new HashMap<String, Integer>();
		for (String rel : relsNeeded) {
			relRows.put(rel, (Integer) catalogs.get(rel).getNumRows());
		}
		// sort the relations by num rows
		ArrayList<String> sortedTables = new ArrayList<String>();
		sort(sortedTables, numRels, relsNeeded, relRows);
		ArrayList<String> tempPreds = new ArrayList<String>(Arrays.asList(preds));
		ArrayList<String> joinOrderSoFar = new ArrayList<String>();
		best = findEntryJoin(tempPreds, relRows, sortedTables);
		while (sortedTables.size() > 0) {
			String toRemove = "";
			// try to find next join from sorted tables
			for (String table : sortedTables) {
				boolean foundPred = false;
				// loop thru all preds to see if this option is valid
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
	// find the join with the largest entry join with largest left child
	public static String findEntryJoin(ArrayList<String> tempPreds, HashMap<String, Integer> relRows, ArrayList<String> sortedTables) {
		int max = Integer.MIN_VALUE;
		int babymax = Integer.MIN_VALUE;
		String currLeftMax = "";
		String currRightMax = "";
		for (String pred : tempPreds) {
			// case where left side of pred has more rows and left side has more than 2nd max
			if (relRows.get(pred.charAt(0)+"") >= max && relRows.get(pred.charAt(1)+"") > babymax) {
				currLeftMax = pred.charAt(0)+"";
				currRightMax = pred.charAt(1)+"";
				max = relRows.get(pred.charAt(0)+"");
				babymax = relRows.get(pred.charAt(1)+"");
			// case where right side of pred has more rows and left side has more than 2nd max
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
	
	public static void main(String[] args) {
		Scanner console = new Scanner(System.in);
		String[] csvPaths = console.nextLine().split(",");
		// map to hold all catalogs
		ConcurrentHashMap<String, Catalog> catalogs = new ConcurrentHashMap<String, Catalog>();
		// compress all csv files concurrently and build map of catalogs
		for (String file : csvPaths) {
			Thread t = new Thread(new ConcurrentLoader(file, catalogs));
			t.start();
		}
		// wait for loads to complete
		while (catalogs.size() != csvPaths.length);
		long start = System.currentTimeMillis();
		String[] rels = {"A", "I", "F", "K", "C", "B", "G"};
		String[] jPreds = {"A.c8 = I.c0", "A.c5 = F.c0", "A.c10 = K.c0", "A.c2 = C.c0", "A.c1 = B.c0", "A.c6 = G.c0"};
		for (Catalog c: catalogs.values()) System.out.println(c.toString());
		String joinOrder = findBestJoinOrder(rels, jPreds, catalogs);
		System.out.println(joinOrder);
		long end = System.currentTimeMillis();
		System.out.println("time = " + (end - start));
		System.out.println();
	}
}
