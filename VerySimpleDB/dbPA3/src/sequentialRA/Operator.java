package sequentialRA;

import java.io.IOException;
import java.util.ArrayList;
import java.util.concurrent.LinkedBlockingQueue;

import utilities.Tuple;
import utilities.TupleInfo;
/**
 * This is the abstract class of all relational algebra operators.
 * @author E K
 *
 */
public abstract class Operator {
	
	private int cardinality = 0;
	private TupleInfo tupleInfo;
	
	public abstract ArrayList<Operator> getChildren();
	public abstract void close() throws IOException;
	public int getCardinality() {
		return cardinality;
	}
	public void setCardinality(int cardinality) {
		this.cardinality = cardinality;
	}
}