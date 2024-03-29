// This class implements a select operator on an input stream of tuples
package concurrentRA;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.ArrayList;
import java.util.concurrent.LinkedBlockingQueue;

import sequentialRA.Filter;
import sequentialRA.Scan;
import utilities.FilterPredicate;
import utilities.Tuple;
import utilities.TupleInfo;

public class ConcurrentFilter extends ConcurrentOperator implements Runnable {

	LinkedBlockingQueue<Tuple> inQ, outQ;
	ConcurrentOperator source;
	protected FilterPredicate[] preds;
	private TupleInfo tupleInfo;
	private ConcurrentColScan src;

	public ConcurrentFilter(String[] preds, ConcurrentOperator source) {
		this.source 	= source;
		tupleInfo  		= source.getTupleInfo();
		this.preds		= buildFilterPreds(preds);
		inQ 			= source.getOutQ();
		outQ 			= new LinkedBlockingQueue<Tuple>(SIZE);
		src 			= (ConcurrentColScan) source;
	}

	@Override
	public void run() {
		int count = 0;
		while (true) {
			Tuple t = null;
			try {
				t = inQ.take();
			} catch (InterruptedException e1) {
				e1.printStackTrace();
			}
			if (t != null) {
				if (t.checkBigPoisonPill()) {
					try {
						outQ.put(new Tuple("poisonPill"));
						break;
					} catch (InterruptedException e) {
						e.printStackTrace();
					}
				} else if (t.checkSmallPoisonPill()) {
					try {
						outQ.put(new Tuple("smallPill"));
						break;
					} catch (InterruptedException e) {
						e.printStackTrace();
					}
				} else {
					boolean passed = true;
					for (int i = 0; i < preds.length; i++) {
						if (!preds[i].test(t)) {
							passed = false;
							break;
						}
					}
					if (passed) {
						try {
							count++;
							outQ.put(t);
						} catch (InterruptedException e) {
							e.printStackTrace();
						}
					}
				}
			}
		}
		close();
	}

	private FilterPredicate[] buildFilterPreds(String[] filterPreds) {
		FilterPredicate[] preds = new FilterPredicate[filterPreds.length];
		for (int i = 0; i < preds.length; i++) {
			preds[i] = new FilterPredicate(filterPreds[i], tupleInfo);
		}
		return preds;
	}

	public LinkedBlockingQueue<Tuple> getOutQ() {
		return outQ;
	}

	@Override
	public void close() {
		inQ = null;
		preds = null;
		src = null;
		source = null;
	}

	@Override
	public ConcurrentColScan getScan() {
		return (ConcurrentColScan) source;
	}

	@Override
	public TupleInfo getTupleInfo() {
		return tupleInfo;
	}
	public String toString() {
		return "FILTER ON " + src.tableName;
	}
}
