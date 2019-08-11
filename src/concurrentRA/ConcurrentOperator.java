package concurrentRA;

import java.io.IOException;
import java.util.concurrent.LinkedBlockingQueue;

import utilities.Tuple;
import utilities.TupleInfo;
/**
 * This is the abstract class of all relational algebra operators.
 * @author E K
 *
 */
public abstract class ConcurrentOperator implements Runnable{

	protected int cardinality = 0;
	protected LinkedBlockingQueue<Tuple> outQ;
	protected boolean exit = false;
	protected final int SIZE = 1000;

	public abstract LinkedBlockingQueue<Tuple> getOutQ();
	public abstract ConcurrentColScan getScan();
	public abstract void close();
	public abstract TupleInfo getTupleInfo();
	public int getCardinality() {
		return cardinality;
	}

	public void setCardinality(int cardinality) {
		this.cardinality = cardinality;
	}
	public abstract String toString();
}
