//This is the main class and entry point of the program
package execution;

import java.io.File;
import java.io.IOException;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.Scanner;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicIntegerArray;
import java.util.concurrent.atomic.AtomicLongArray;

import javax.swing.plaf.synth.SynthScrollBarUI;

import utilities.Catalog;

public class DB {

	public static void main(String[] args) throws IOException {
		new File("dataConverted").mkdir();
		Scanner console = new Scanner(System.in);
		String[] csvPaths = console.nextLine().split(",");
		// map to hold all catalogs
		ConcurrentHashMap<String, Catalog> catalogs = new ConcurrentHashMap<String, Catalog>();
		// long loadstart = System.currentTimeMillis();
		// compress all csv files concurrently and build map of catalogs
		for (String file : csvPaths) {
			Thread t = new Thread(new ConcurrentColLoader(file, catalogs));
			t.start();
		}
		// wait for loads to complete
		while (catalogs.size() != csvPaths.length);
		for (Catalog c : catalogs.values()) System.out.println(c.toString());
		// System.out.println("load took " + (System.currentTimeMillis() - loadstart));
		// grab all queries
		QueryParser parser = new QueryParser(console);
		parser.parse();
		long start = System.currentTimeMillis();
		for (Query query : parser.getQueries()) {
			Pipeline queryPipeline = Optimizer.buildPipeline(query, catalogs);
			AtomicLongArray sums = queryPipeline.sums;
			queryPipeline.execute();
			// wait till pipeline complete
			while (sums.get(query.sumsNeeded.length) == 0L);
			boolean receivedSums = false;
			for (int j = 0; j < query.sumsNeeded.length; j ++) {
				if (sums.get(j) != 0L) receivedSums = true;
			}
			for (int i = 0; i < query.sumsNeeded.length; i++) {
				if (receivedSums) System.out.print(sums.get(i));
				if (i < query.sumsNeeded.length - 1 || (query.sumsNeeded.length == 2 && i == 0)) System.out.print(",");
			}
			System.out.println();
		}
		System.out.println("Query took: " + (System.currentTimeMillis() - start));
	}
}
