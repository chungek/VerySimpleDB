package sequentialRA;

import java.util.ArrayList;

import utilities.FilterPredicate;
import utilities.Tuple;
import utilities.TupleInfo;
/**
 * This is the class file for the filter operator.
 * @author E K
 *
 */
public class Filter extends Operator {
	
	private Scan source;
	protected FilterPredicate p;
	private TupleInfo tupleInfo;
	private Scan scan;
	
	public Filter(FilterPredicate p, TupleInfo tupleInfo) {
		this.p 			= p;
		this.tupleInfo  = tupleInfo;
	}
	
	public void init(Scan source) {
		this.source = source;
	}
	
	public boolean hasNext() {
		return source.hasNext();
	}
	
	public Tuple next() {
		Tuple input = source.next();
		if (p.test(input)) return input;
		return null;
	}

	@Override
	public void close() {
		// TODO Auto-generated method stub
		
	}

	@Override
	public ArrayList<Operator> getChildren() {
		// TODO Auto-generated method stub
		return null;
	}
}
