// This class loads a csv file and creates a new directory as a dump for .dat files for each column
package execution;

import java.io.BufferedOutputStream;
import java.io.BufferedReader;
import java.io.DataOutputStream;
import java.io.File;
import java.io.FileOutputStream;
import java.io.FileReader;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.CharBuffer;
import java.nio.channels.FileChannel;
import java.util.ArrayList;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.LinkedBlockingQueue;

import concurrentRA.ConcurrentOperator;
import concurrentRA.ConcurrentScan;
import utilities.Catalog;
import utilities.Tuple;
import utilities.TupleInfo;

public class ConcurrentColLoader implements Runnable {

	private String csvPath;
	private final int PAGE_SIZE;
	private ConcurrentHashMap<String, Catalog> catalogs;

	public ConcurrentColLoader(String csvPath, ConcurrentHashMap<String, Catalog> catalogs) {
		PAGE_SIZE 	  = 10 * 1024 * 1024;
		this.csvPath  = csvPath;
		this.catalogs = catalogs;
	}
	private void compress() throws IOException {
		BufferedReader br = new BufferedReader(new FileReader(csvPath), PAGE_SIZE);
		String[] splitSlash = csvPath.replace(".csv", ".dat").split("/");
		String newdir = "dataConverted/" + splitSlash[splitSlash.length - 1].charAt(0);
		new File(newdir).mkdir();
		String relationName = splitSlash[splitSlash.length - 1].charAt(0)+"";
		FileChannel[] outChannels = null;
		CharBuffer cb1 = CharBuffer.allocate(PAGE_SIZE);
		CharBuffer cb2 = CharBuffer.allocate(PAGE_SIZE);
		ByteBuffer[] outBuffers = null;
		boolean firstPass = true;
		int colCount = 0;
		int rowCount = 0;
		int colCounter = 0;
		ArrayList<Integer> firstTuple = new ArrayList<Integer>();
		int writes = 0;
		boolean skip = false;
		while (br.read(cb1) != -1) {
			cb1.flip();
			int lastNumStart = 0;
			for (int i = 0; i < cb1.length(); i++) {
				if (cb1.charAt(i) == ',' || cb1.charAt(i) == '\n') {
					// count num of cols for catalog
					if (firstPass) {
						if (cb1.charAt(i) == ',') {
							colCount++;
						} else if (cb1.charAt(i) == '\n') {
							colCount++;
							firstPass = false;
							outChannels = new FileChannel[colCount];
							outBuffers = new ByteBuffer[colCount];
							// init each output file
							for (int j = 0; j < colCount; j++) {
								String diskLocation = newdir + "/" + j;
								FileOutputStream fos = new FileOutputStream(diskLocation);
								outChannels[j] = fos.getChannel();
								outBuffers[j] = ByteBuffer.allocateDirect(PAGE_SIZE);
								if (j == colCount - 1) {
									String temp = cb1.subSequence(lastNumStart, i).toString();
									int numRead = Integer.parseInt(temp, 10);
									outBuffers[j].putInt(numRead);
									lastNumStart = i + 1;
								} else {
									outBuffers[j].putInt(firstTuple.get(j));
								}
							}
							colCounter = 0;
							rowCount++;
							skip = true;
						}
					}
					if (skip) {
						skip = false;
						continue;
					}
					String temp = cb1.subSequence(lastNumStart, i).toString();
					int numRead = Integer.parseInt(temp, 10);
					// count num of rows for catalog
					if (cb1.charAt(i) == '\n') {
						rowCount++;
					}
					if (!firstPass && !outBuffers[colCounter].hasRemaining()) {
						writes++;
						writeToDisk(outBuffers, outChannels, colCount);
					}
					if (firstPass) {
						firstTuple.add(numRead);
					} else {
						outBuffers[colCounter].putInt(numRead);
					}
					if (colCounter < colCount - 1) {
						colCounter++;
					} else {
						colCounter = 0;
					}
					lastNumStart = i + 1;
				}
			}
			// flip buffers
			cb2.clear();
			cb2.append(cb1, lastNumStart, cb1.length());
			CharBuffer tmp = cb2;
			cb2 = cb1;
			cb1 = tmp;
		}
		if (outBuffers[0].hasRemaining()) {
			writes++;
			writeToDisk(outBuffers, outChannels, colCount);
		}
		catalogs.put(relationName, new Catalog(newdir, relationName, colCount, rowCount));
		br.close();
		for (FileChannel fc : outChannels) {
			fc.close();
		}
		for (ByteBuffer bb : outBuffers) bb = null;
	}

	private void writeToDisk(ByteBuffer[] outBuffers, FileChannel[] outChannels, int colCount) throws IOException {
		for (int i = 0; i < colCount; i++) {
			outBuffers[i].flip();
			outChannels[i].write(outBuffers[i]);
			outBuffers[i].clear();
		}
	}

	public void run() {
		try {
			compress();
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}
	@Override
	public String toString() {
		// TODO Auto-generated method stub
		return null;
	}
}
