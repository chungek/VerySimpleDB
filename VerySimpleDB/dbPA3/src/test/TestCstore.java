package test;

import java.io.File;
import java.io.IOException;
import java.util.Scanner;
import java.util.concurrent.ConcurrentHashMap;

import concurrentRA.ConcurrentColScan;
import concurrentRA.ConcurrentFilter;
import concurrentRA.ConcurrentScan;
import concurrentRA.PrintOutQ;
import execution.ConcurrentColLoader;
import execution.ConcurrentLoader;
import utilities.Catalog;
import utilities.TupleInfo;
import utilities.Utils;

public class TestCstore {
	public static void main(String[] args) throws IOException {
		new File("dataConverted").mkdir();
		Scanner console = new Scanner(System.in);
		String[] csvPaths = console.nextLine().split(",");
		//map to hold all catalogs
		ConcurrentHashMap<String, Catalog> catalogs = new ConcurrentHashMap<String, Catalog>();
		//compress all csv files concurrently and build map of catalogs
		long start = System.currentTimeMillis();
		for (String file : csvPaths) {
			Thread t = new Thread(new ConcurrentColLoader(file, catalogs));
			t.start();
		}
		//wait for loads to complete
		while (catalogs.size() != csvPaths.length);
		System.out.println("Compression took: " + (System.currentTimeMillis() - start));
		for (Catalog c : catalogs.values()) System.out.println(c.toString());
		String[] colsA = {"0", "1", "2", "3", "4", "5", "6", "7", "8", "9"};
		ConcurrentColScan scanA = new ConcurrentColScan(catalogs.get("A"), true, colsA);
//		String[] fpreds = {"A.c0 > 8000"};
//		ConcurrentFilter filt = new ConcurrentFilter(fpreds, scanA);
//		String[] colsB = {"0", "1", "2", "3"};
//		ConcurrentColScan scanB = new ConcurrentColScan(catalogs.get("B"), false, colsB);
		PrintOutQ printer = new PrintOutQ(scanA);
		new Thread(scanA).start();
//		new Thread(filt).start();
		new Thread(printer).start();
		
	}
}
