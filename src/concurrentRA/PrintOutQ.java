// This is just a class to print out tuples for testing/debugging
package concurrentRA;

import java.io.IOException;
import java.util.ArrayList;
import java.util.concurrent.LinkedBlockingQueue;

import utilities.Tuple;
import utilities.TupleInfo;


public class PrintOutQ extends ConcurrentOperator implements Runnable {

	public LinkedBlockingQueue<Tuple> inQ;
	public ConcurrentOperator source;
	TupleInfo tupleInfo;
	private boolean exit;

	public PrintOutQ(ConcurrentOperator source) {
		this.source = source;
		inQ 		= source.getOutQ();
		tupleInfo 	= source.getTupleInfo();
		exit 		= false;
	}

	@Override
	public void run() {
		int count = 0;
		long start = System.currentTimeMillis();
		while (!exit) {
			Tuple t = inQ.poll();
			if (t != null) {
				if (t.checkBigPoisonPill()) {
					System.out.println("printer received pill");
					close();
					exit = true;
				} else {
					count++;
					if (count < 10 || (count > 300000 && count < 300010)) {
						System.out.println(count + " " + t.toString());
					}
				}
			}
		}
		long end = System.currentTimeMillis();
		System.out.println("num tuples that made it to last Q: " + count);
		System.out.println("total query time: " + (end - start));
		System.out.println("print dead");
	}

	@Override
	public LinkedBlockingQueue<Tuple> getOutQ() {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public void close() {
		inQ.drainTo(new ArrayList<Tuple>());
		source.close();
		exit = true;
	}

	@Override
	public ConcurrentColScan getScan() {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public TupleInfo getTupleInfo() {
		return tupleInfo;
	}

	@Override
	public String toString() {
		// TODO Auto-generated method stub
		return null;
	}
}
