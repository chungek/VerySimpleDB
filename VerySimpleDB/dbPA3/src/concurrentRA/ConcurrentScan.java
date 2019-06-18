// This class is a scan operator that sends tuples from a .dat file to the operator tree
// When this object reaches the end of files it is reading, it sends a poison pill up to its parent to indicate that
// it is done, there so that the parent can expect no more tuples from this thread. It will send a *BIG* pill if it
// is the left most child of the RA tree, and a *small* pill if it is a right child of a subtree rooted in a join.
package concurrentRA;

import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.util.concurrent.LinkedBlockingQueue;
import interfacesRA.ScanFunctions;
import utilities.Catalog;
import utilities.Tuple;
import utilities.TupleInfo;

public class ConcurrentScan extends ConcurrentOperator implements ScanFunctions, Runnable {

	private LinkedBlockingQueue<Tuple> outQ;
	public String tableName;
	private String relationPath;
	private int numCols;
	private final int BUFFER_CAPACITY;
	protected FileChannel ch;
	private ByteBuffer buffer;
	private byte[] tupleAsBytes;
	private int bufferSize;				/* pointer to last byte in buffer */
	private int tupleSize;				/* number of bytes to make a tuple */
	public boolean isDone;
	private int numRows;
	public int numTuplesInBuffer;		/* max number of tuples to be sent to next operator */
	private TupleInfo tupleInfo;
	public boolean entryPoint;

	public ConcurrentScan(Catalog catalog,  boolean entryPoint) throws IOException {
		outQ 							= new LinkedBlockingQueue<Tuple>(SIZE);
		tableName 				= catalog.relationName;
		relationPath 			= catalog.getRelationPath();
		numCols 					= catalog.getNumCols();
		BUFFER_CAPACITY 	= 200 * 4 * 1024;
		tupleSize 				= numCols * 4;
		ch 								= new FileInputStream(relationPath).getChannel();
		buffer 						= ByteBuffer.allocate(BUFFER_CAPACITY);
		bufferSize 				= 0;
		numRows 					= catalog.getNumRows();
		numTuplesInBuffer	= BUFFER_CAPACITY / (numCols * 4);
		tupleInfo 				= catalog.getTupleInfo();
		this.entryPoint		= entryPoint;
	}

	@Override
	public void run() {
		while (!exit) {
			int count = 0;
			//scans entire table
			while (this.hasNext()) {
				if (exit) break;
				try {
					outQ.put(this.next());
					count++;
				} catch (InterruptedException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				}
			}
			if (exit) break;
			//if the start of the pipeline is done, we can send in the poisonpill to shut everything down
			if (entryPoint) {
				try {
					outQ.put(new Tuple("poisonPill"));
					close();
					break;
				} catch (InterruptedException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				}
			} else {
				try {
					outQ.put(new Tuple("smallPill"));
					reBuffer();
				} catch (InterruptedException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				} catch (IOException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				}
			}
		}
		System.out.println("scan dead");
	}
	public LinkedBlockingQueue<Tuple> getOutQ() {
		return outQ;
	}

	@Override
	public void close() {
		exit = true;
	}

	@Override
	public boolean hasNext() {
		if (tupleAsBytes == null) {
			try {
				refillBuffer();
			} catch (IOException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
		}
		if (!buffer.hasRemaining()) {
			buffer.clear();
			try {
				refillBuffer();
			} catch (IOException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
			if (bufferSize == -1) {
				try {
					ch.close();
				} catch (IOException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				}
				this.isDone = true;
				return false;
			}
		}
		return true;
	}

	@Override
	public Tuple next() {
		tupleAsBytes = new byte[tupleSize];
		for (int j = 0; j < tupleSize; j++) {
			if (!buffer.hasRemaining()) {
				buffer.clear();
				try {
					refillBuffer();
				} catch (IOException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				}
			}
			tupleAsBytes[j] = buffer.get();
		}
		return new Tuple(tupleAsBytes, numCols, tupleSize);
	}

	@Override
	public void refillBuffer() throws IOException {
		buffer.clear();
		bufferSize = ch.read(buffer);
		if (bufferSize != -1) {
			buffer.position(0);
			buffer.limit(bufferSize);
		}
	}

	@Override
	public void reBuffer() throws FileNotFoundException {
		try {
			ch.close();
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		ch = new FileInputStream(relationPath).getChannel();
		buffer.clear();
		tupleAsBytes = null;
		isDone = false;
	}

	@Override
	public int getNumRows() {
		return numRows;
	}

	@Override
	public TupleInfo getTupleInfo() {
		return tupleInfo;
	}

	@Override
	public ConcurrentColScan getScan() {
		return null;
	}

	public String toString() {
		return "SCAN " + tableName;
	}
}
