// This class is a scanner for a col based file directory
package concurrentRA;

import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.util.ArrayList;
import java.util.concurrent.LinkedBlockingQueue;

import utilities.Catalog;
import utilities.Tuple;
import utilities.TupleInfo;

/**
 * concurrent scanner than scans the col store of a relation and builds the tuples
 * @author E K
 *
 */
public class ConcurrentColScan extends ConcurrentOperator implements Runnable {
	private LinkedBlockingQueue<Tuple> outQ;
	public String tableName;
	private String relationPath;
	private int numCols;
	private final int BUFFER_CAPACITY;
	protected FileChannel[] inChannels;
	private ByteBuffer[] buffers;
	private byte[] tupleAsBytes;
	private int bufferSize;							/* pointer to last byte in buffer */
	private int tupleSize;							/* number of bytes to make a tuple */
	public boolean isDone;
	private int numRows;
	public int numTuplesInBuffer;				/* max number of tuples to be sent to next operator */
	private TupleInfo tupleInfo;
	public boolean entryPoint;

	public ConcurrentColScan(Catalog catalog,  boolean entryPoint, String[] allColsNeeded) throws IOException {
		outQ 							= new LinkedBlockingQueue<Tuple>(SIZE);
		tableName 				= catalog.relationName;
		numCols 				= allColsNeeded.length;
		relationPath 			= catalog.getRelationPath();
		BUFFER_CAPACITY 		= 20 * 1024 * 1024;
		tupleSize 				= numCols * 4;
		bufferSize 				= 0;
		numRows 				= catalog.getNumRows();
		numTuplesInBuffer		= BUFFER_CAPACITY / (numCols * 4);
		tupleInfo 				= buildTupleInfo(allColsNeeded, catalog.relationName);
		this.entryPoint			= entryPoint;
		init(allColsNeeded);
	}
	@Override
	public void run() {
		while (this.hasNext()) {
			Tuple t = this.next();
			try {
				outQ.put(t);
			} catch (InterruptedException e) {
				e.printStackTrace();
			}
		}
		if (entryPoint) {
			try {
				outQ.put(new Tuple("poisonPill"));
			} catch (InterruptedException e) {
				e.printStackTrace();
			}
		} else {
			try {
				outQ.put(new Tuple("smallPill"));
			} catch (InterruptedException e) {
				e.printStackTrace();
			}
		}
		close();
	}

	private void init(String[] allColsNeeded) throws FileNotFoundException {
		inChannels 	= new FileChannel[numCols];
		buffers		= new ByteBuffer[numCols];
		for (int i = 0; i < numCols; i++) {
			inChannels[i] = new FileInputStream(relationPath + "/" + allColsNeeded[i]).getChannel();
			buffers[i] = ByteBuffer.allocate(BUFFER_CAPACITY);
		}
	}
	public static TupleInfo buildTupleInfo(String[] allColsNeeded, String relName) {
		ArrayList<Integer> actualIndices = new ArrayList<Integer>();
		for (String col : allColsNeeded) actualIndices.add(Integer.parseInt(col));
		return new TupleInfo(allColsNeeded.length, relName, actualIndices);
	}

	public LinkedBlockingQueue<Tuple> getOutQ() {
		return outQ;
	}

	@Override
	public void close() {
		for (FileChannel fc : inChannels) {
			try {
				fc.close();
			} catch (IOException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
		}
		inChannels = null;
		buffers = null;
	}

	private boolean hasNext() {
		//case at init
		if (tupleAsBytes == null) {
			try {
				refillBuffers();
			} catch (IOException e) {
				e.printStackTrace();
			}
		} else if (!buffers[0].hasRemaining()) {
			try {
				refillBuffers();
			} catch (IOException e) {
				e.printStackTrace();
			}
			if (bufferSize == -1) {
				try {
					for (FileChannel ch : inChannels) ch.close();
					exit = true;
				} catch (IOException e) {
					e.printStackTrace();
				}
				return false;
			}
		}
		return true;
	}

	private Tuple next() {
		tupleAsBytes = new byte[tupleSize];
		for (int j = 0; j < numCols; j++) {
			if (!buffers[j].hasRemaining()) {
				try {
					refillBuffers();
				} catch (IOException e) {
					e.printStackTrace();
				}
			}
			int start = j * 4;
			for (int i = start; i < start + 4; i++) {
				tupleAsBytes[i] = buffers[j].get();
			}
		}
		return new Tuple(tupleAsBytes, numCols, tupleSize);
	}
	public void refillBuffers() throws IOException {
		for (int i = 0; i < numCols; i++) {
			buffers[i].clear();
			bufferSize = inChannels[i].read(buffers[i]);
			if (bufferSize != -1) {
				buffers[i].position(0);
				buffers[i].limit(bufferSize);
			} else if (bufferSize == 1 && i == numCols - 1) {
				isDone = true;
			}
		}
	}

	public int getNumRows() {
		return numRows;
	}

	@Override
	public TupleInfo getTupleInfo() {
		return tupleInfo;
	}

	public ConcurrentColScan getScan() {
		return this;
	}

	public String toString() {
		return "SCAN " + tableName;
	}
}
