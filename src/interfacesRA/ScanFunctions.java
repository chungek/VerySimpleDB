package interfacesRA;

import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;

import utilities.Tuple;
import utilities.TupleInfo;

public interface ScanFunctions {
	/**
	 * method to check if there is a next tuple in the relation
	 * @return - true if there is next line, false if there isn't
	 */
	public boolean hasNext();
	/**
	 * this method returns the next list of tuples
	 */
	public Tuple next();
	/**
	 * method to refill the buffer and re-calibrate the buffer pointers
	 * @throws IOException
	 */
	public void refillBuffer() throws IOException;
	public void reBuffer() throws FileNotFoundException;
	public int getNumRows();
	public TupleInfo getTupleInfo();
}
