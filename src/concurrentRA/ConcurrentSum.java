// This class is the sum operator that will be at the root of the RA tree
package concurrentRA;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.atomic.AtomicIntegerArray;
import java.util.concurrent.atomic.AtomicLongArray;

import utilities.Tuple;
import utilities.TupleInfo;
import utilities.Utils;

public class ConcurrentSum extends ConcurrentOperator implements Runnable {

	private ConcurrentOperator source;
	private LinkedBlockingQueue<Tuple> inQ;
	private TupleInfo tupleInfo;
	private	AtomicLongArray sums;
	public int[] sumIndices;
	private boolean exit;

	/**
	 *
	 * @param source
	 * @param sumpreds - in the form of {"SUM(D.c0)", "SUM(D.c4)", "SUM(C.c1)"}
	 * @param sums
	 */
	public ConcurrentSum(ConcurrentOperator source, String[] sumPreds, AtomicLongArray sums) {
		this.source 		= source;
		this.sums				= sums;
		this.tupleInfo 	= source.getTupleInfo();
		inQ 						= source.getOutQ();
		exit 						= false;
		sumIndices 			= getSumIndices(sumPreds);
	}

	@Override
	public void run() {
		Tuple t;
		long[] runningSum = new long[sumIndices.length];
		while (true) {
			t = inQ.poll();
			if (t != null) {
				if (t.checkBigPoisonPill()) {
					for (int i = 0; i < runningSum.length; i++) {
						sums.getAndSet(i, runningSum[i]);
					}
					sums.getAndIncrement(sumIndices.length);
					break;
				} else {
					for (int i = 0; i < runningSum.length; i++) {
						runningSum[i] += t.getValue(sumIndices[i]);
					}
				}
			}
		}
	}

	public int[] getSumIndices(String[] sumPreds) {
		sumIndices = new int[sumPreds.length];
		int count = 0;
		for (String currPred : sumPreds) {
			String colInfo = currPred.replace("SUM(", "").replace(")", "");
			sumIndices[count] = Utils.map(tupleInfo, colInfo);
			count++;
		}
		return sumIndices;
	}

	@Override
	public LinkedBlockingQueue<Tuple> getOutQ() {
		return null;
	}

	@Override
	public ConcurrentColScan getScan() {
		return null;
	}

	@Override
	public void close() {
		inQ = null;
		source = null;
	}

	@Override
	public TupleInfo getTupleInfo() {
		return tupleInfo;
	}
	public String toString() {
		return "SUM ON " + source.toString();
	}
}
