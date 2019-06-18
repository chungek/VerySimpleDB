package sequentialRA;

import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.util.ArrayList;
import utilities.Catalog;
import utilities.Tuple;
import utilities.TupleInfo;

/**
 * This is the class file of the table scanner.
 * @author E K
 *
 */
public class Scan extends Operator {
	
	private String relationPath;
	private int numCols;
	private final int BUFFER_CAPACITY;
	protected FileChannel ch;
	private ByteBuffer buffer;
	private byte[] tupleAsBytes;
	private int bufferSize;			/* pointer to last byte in buffer */
	private int tupleSize;			/* number of bytes to make a tuple */
	public boolean isDone;
	private int numRows;
	private boolean entryPoint;		/* keep track of whether is initial scan */
	public int numTuplesInBuffer;	/* max number of tuples to be sent to next operator */
	private TupleInfo tupleInfo;
	private int numRowsLeft;
	private Operator next;
	
	public Scan(Catalog catalog, boolean entryPoint) throws IOException {
		
		this.relationPath 	= catalog.getRelationPath();
		numCols 			= catalog.getNumCols();
		BUFFER_CAPACITY 	= 4 * 1024;
		tupleSize 			= numCols * 4;
		ch 					= new FileInputStream(relationPath).getChannel();
		buffer 				= ByteBuffer.allocate(BUFFER_CAPACITY);
		bufferSize 			= 0;
		isDone 				= false;
		numRows 			= catalog.getNumRows();
		numTuplesInBuffer 	= BUFFER_CAPACITY / (numCols * 4);
//		tupleInfo 			= new TupleInfo(numCols, catalog.relationName);
	}
	/**
	 * method to check if there is a next tuple in the relation
	 * @return - true if there is next line, false if there isn't
	 */
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
	/**
	 * this method returns the next list of tuples
	 */
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
	/**
	 * method to refill the buffer and re-calibrate the buffer pointers
	 * @throws IOException
	 */
	private void refillBuffer() throws IOException {
		buffer.clear();
		bufferSize = ch.read(buffer);
		if (bufferSize != -1) {
			buffer.position(0);
			buffer.limit(bufferSize);
		}
	}
	public void reBuffer() throws FileNotFoundException {
		ch = new FileInputStream(relationPath).getChannel();
		tupleAsBytes = null;
		isDone = false;
		numRowsLeft = numRows;
	}

	public int getNumRows() {
		return numRows;
	}
	public TupleInfo getTupleInfo() {
		return tupleInfo;
	}
	@Override
	public void close() throws IOException {
		ch.close();
	}
	@Override
	public ArrayList<Operator> getChildren() {
		// TODO Auto-generated method stub
		return null;
	}
	public void setNext(Operator o) {
		next = o;
	}
}
