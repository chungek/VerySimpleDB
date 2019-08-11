// This class is the join operator
// A *BIG* poison pill means that the left most child of the RA tree has scanned to end of file and there will be no
// more tuples coming up the pipeline. "SMALL" pill means that scan of its right child's subtree has read to end of file
// and will not send any more tuples to be added to the hash map.
package concurrentRA;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.Map.Entry;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.atomic.AtomicBoolean;

import utilities.JoinPredicate;
import utilities.Tuple;
import utilities.TupleInfo;

public class ConcurrentJoin extends ConcurrentOperator implements Runnable {

	private ConcurrentOperator c1, c2;
	private LinkedBlockingQueue<Tuple> source1, source2, outQ;
	private JoinPredicate[] preds;
	private TupleInfo tupleInfo;
	private HashMap<Integer, LinkedList<Tuple>> tupleMapOfC2;
	boolean buildDone, done;
	private int c1HashKey, c2HashKey;
	int passedcount;

	public ConcurrentJoin(ConcurrentOperator c1, ConcurrentOperator c2, JoinPredicate[] preds) {
		this.c1 		= c1;
		this.c2 		= c2;
		this.source1 	= c1.getOutQ();
		this.source2 	= c2.getOutQ();
		this.preds 		= preds;
		tupleInfo  		= new TupleInfo(c1.getTupleInfo(), c2.getTupleInfo());
		outQ 			= new LinkedBlockingQueue<Tuple>(SIZE);
		exit 			= false;
		tupleMapOfC2	= new HashMap<Integer, LinkedList<Tuple>>();
		buildDone 		= false;
		done 			= false;
		c1HashKey		= preds[0].index1;
		c2HashKey		= preds[0].index2;
		passedcount		= 0;
	}

	@Override
	public void run() {
		buildMap();
		while (true) {
			Tuple t1 = null;
			try {
				t1 = source1.take();
			} catch (InterruptedException e1) {
				e1.printStackTrace();
			}
			if (t1 != null ) {
				if (t1.checkBigPoisonPill()) {
					try {
						outQ.put(new Tuple("poisonPill"));
						break;
					} catch (InterruptedException e) {
						e.printStackTrace();
					}
				} else {
					join(t1);
				}
			}
		}
		close();
	}

	public void join(Tuple t1) {
		if (tupleMapOfC2.containsKey(t1.getValue(c1HashKey))) {
			LinkedList<Tuple> matches = tupleMapOfC2.get(t1.getValue(c1HashKey));
			// multiple join preds to check
			if (preds.length > 1) {
				Iterator<Tuple> checkItr = matches.iterator();
				LinkedList<Tuple> failedList = new LinkedList<Tuple>();
				while (checkItr.hasNext()) {
					Tuple t2 = checkItr.next();
					boolean failed = false;
					for (int i = 1; i < preds.length; i++) {
						if (!preds[i].test(t1, t2)) {
							failedList.add(t2);
							break;
						}
					}
				}
				if (failedList.size() > 0) matches.removeAll(failedList);
				if (matches.size() > 0) {
					Iterator<Tuple> itr = matches.iterator();
					while (itr.hasNext()) {
						try {
							Tuple t2ForJoin = itr.next();
							Tuple joined = new Tuple(t1, t2ForJoin);
							passedcount++;
							outQ.put(joined);
						} catch (InterruptedException e) {
							e.printStackTrace();
						}
					}
				}
			} else {
				for (Tuple t2 : matches) {
					try {
						Tuple joined = new Tuple(t1, t2);
						passedcount++;
						outQ.put(joined);
					} catch (InterruptedException e) {
						e.printStackTrace();
					}
				}
			}
		}
	}

	public void buildMap() {
		while (!buildDone) {
			Tuple t2 = null;
			try {
				t2 = source2.take();
			} catch (InterruptedException e) {
				e.printStackTrace();
			}
			if (t2 != null) {
				if (t2.checkSmallPoisonPill()) {
					buildDone = true;
				} else {
					LinkedList<Tuple> temp = new LinkedList<Tuple>();
					temp.add(t2);
					if (tupleMapOfC2.containsKey(t2.getValue(c2HashKey))) {
						temp.addAll(tupleMapOfC2.get(t2.getValue(c2HashKey)));
					}
					tupleMapOfC2.put(t2.getValue(c2HashKey), temp);
				}
			}
		}
	}

	@Override
	public void close() {
		source1 = null;
		source2 = null;
		c1 = null;
		c2 = null;
		tupleMapOfC2 = null;
	}

	@Override
	public LinkedBlockingQueue<Tuple> getOutQ() {
		return this.outQ;
	}

	@Override
	public ConcurrentColScan getScan() {
		return null;
	}

	@Override
	public TupleInfo getTupleInfo() {
		return tupleInfo;
	}
	public String toString() {
		return "JOIN ON " + c2.toString();
	}
}
