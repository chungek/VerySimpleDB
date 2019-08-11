//This class converts a csv file to a .dat file
package execution;

import java.io.BufferedOutputStream;
import java.io.BufferedReader;
import java.io.DataOutputStream;
import java.io.FileOutputStream;
import java.io.FileReader;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.CharBuffer;
import java.nio.channels.FileChannel;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.concurrent.ConcurrentHashMap;

import utilities.Catalog;

/**
 * This is the class file for the loader.
 * @author E K
 *
 */
public class ConcurrentLoader implements Runnable {

	private String csvPath;
	private final int PAGE_SIZE;
	private ConcurrentHashMap<String, Catalog> catalogs;
	public ConcurrentLoader(String csvPath, ConcurrentHashMap<String, Catalog> catalogs) {
		PAGE_SIZE 	  = 64 * 1024;
		this.csvPath  = csvPath;
		this.catalogs = catalogs;
	}
	/**
	 * This method compresses a csv file with each 32bit int represented
	 * in 4 bytes in a dat file, then adding a catalog of that file to a
	 * hashmap in the driver method
	 * @throws IOException
	 */
	private void compress() throws IOException {
		BufferedReader br = new BufferedReader(new FileReader(csvPath), PAGE_SIZE);
		String[] splitSlash = csvPath.replace(".csv", ".dat").split("/");
		String resultPath = "dataConverted/" + splitSlash[splitSlash.length - 1];
		FileOutputStream fos = new FileOutputStream(resultPath);
		FileChannel outChannel = fos.getChannel();
		DataOutputStream dos = new DataOutputStream(new BufferedOutputStream(fos));
		CharBuffer cb1 = CharBuffer.allocate(PAGE_SIZE);
		CharBuffer cb2 = CharBuffer.allocate(PAGE_SIZE);
		// allocate direct * 200 works well on L
		ByteBuffer bb = ByteBuffer.allocateDirect(PAGE_SIZE * 200);
		boolean firstPass = true;
		int colCount = 0;
		int rowCount = 0;
		//write buffers to .dat file
		int writes = 0;
		while (br.read(cb1) != -1) {
			cb1.flip();
			int lastNumStart = 0;
			for (int i = 0; i < cb1.length(); i++) {
				if (cb1.charAt(i) == ',' || cb1.charAt(i) == '\n') {
					//count num of cols for catalog
					if (firstPass) {
						if (cb1.charAt(i) == ',') {
							colCount++;
						} else if (cb1.charAt(i) == '\n') {
							colCount++;
							firstPass = false;
						}
					}
					//count num of rows for catalog
					if (cb1.charAt(i) == '\n') {
						rowCount++;
					}
					String temp = cb1.subSequence(lastNumStart, i).toString();
					int numRead = Integer.parseInt(temp, 10);
					if (!bb.hasRemaining()) {
						writes++;
						writeToDisk(bb, outChannel);
					}
					bb.putInt(numRead);
					lastNumStart = i + 1;
				}
			}
			//flip buffers
			cb2.clear();
			cb2.append(cb1, lastNumStart, cb1.length());
			CharBuffer tmp = cb2;
			cb2 = cb1;
			cb1 = tmp;
		}
		if (bb.hasRemaining()) {
			writes++;
			writeToDisk(bb, outChannel);
		}
		String temp = resultPath.replace(".dat", "");
		String relationName = temp.substring(temp.length()-1);
		catalogs.put(relationName, new Catalog(resultPath, relationName, colCount, rowCount));
		br.close();
		dos.close();
		outChannel.close();
	}

	private void writeToDisk(ByteBuffer bb, FileChannel channel) throws IOException {
		bb.flip();
		channel.write(bb);
		bb.clear();
	}

	@Override
	public void run() {
		try {
			compress();
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}
}
