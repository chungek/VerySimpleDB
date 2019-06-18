//This class represents the pipeline that data flows through to the root of the tree
//The execute method starts the threads and begins executing the query
package execution;

import java.util.LinkedList;
import java.util.concurrent.atomic.AtomicIntegerArray;
import java.util.concurrent.atomic.AtomicLongArray;

public class Pipeline {

//	LinkedList<Thread> threads;
	LinkedList<Thread> entryAndExit, rightChildScans, joinsAndRightFilts;
	public AtomicLongArray sums;

	public Pipeline(LinkedList<Thread> entryAndExit, LinkedList<Thread> joinsAndRightFilts,
									LinkedList<Thread> rightChildScans, AtomicLongArray sums) {
		this.entryAndExit = entryAndExit;
		this.joinsAndRightFilts = joinsAndRightFilts;
		this.rightChildScans = rightChildScans;
		this.sums	 = sums;
	}

	public void execute() {

		for (Thread rightScan : rightChildScans) {
			rightScan.start();
		}

		for (Thread joinOrRightFilt : joinsAndRightFilts) {
			joinOrRightFilt.start();
		}

		LinkedList<Thread> poolToCheck = new LinkedList<Thread>();
		poolToCheck.addAll(rightChildScans);
		poolToCheck.addAll(joinsAndRightFilts);

		while (setUpNotDone(poolToCheck));

		for (Thread firstAndLast : entryAndExit) {
			firstAndLast.start();
		}
	}
	public boolean setUpNotDone(LinkedList<Thread> poolToCheck) {
		for (Thread t : poolToCheck) {
			if (t.isAlive()) return false;
		}
		return true;
	}
}
