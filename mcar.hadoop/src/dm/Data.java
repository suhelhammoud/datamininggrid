package dm;

import org.apache.log4j.Logger;

import others.Tools;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.FileReader;
import java.io.FileWriter;
import java.math.BigInteger;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;

import filters.IMap;

public class Data {
	/**
	 * Logger for this class
	 */
	private static final Logger logger = Logger.getLogger(Data.class);

	protected String relation = "";

	protected List<String> attributes = new ArrayList<String>();
	/**
	 * line number: line integer values
	 */
	protected Map<Integer, int[]> dataMap = new TreeMap<Integer, int[]>();// entity
	/**
	 * for every attribute there is an IMap
	 */
	protected List<IMap> imaps = new ArrayList<IMap>();

	public Data() {

	}

	public Data(String fileName) {
		readFromFile(fileName);
	}

	public boolean readFromFile(String fileName) {

		attributes.clear();
		dataMap.clear();
		imaps.clear();

		int errorLine = 1;
		try {
			BufferedReader in = new BufferedReader(new FileReader(fileName));

			String s = "";
			while (!s.startsWith("@attribute")) {
				s = in.readLine().toLowerCase().trim();
			}

			// read till @data line
			int numOfCols = 0;
			while (!s.startsWith("@data")) {
				if (s.startsWith("@attribute")) {
					attributes.add(s.split("\\s+")[1]);// \\s+ one or more
														// spaces
					imaps.add(new IMap());
					numOfCols++;
				}
				s = in.readLine().toLowerCase().trim();
			}

			// read data
			int lineNumber = 1;
			while ((s = in.readLine()) != null) {

				final String[] arr = s.trim().split(",");

				int[] row = new int[arr.length];
				for (int i = 0; i < arr.length; i++) {
					row[i] = imaps.get(i).mapValue(arr[i], lineNumber);
				}
				dataMap.put(lineNumber++, row);
				errorLine++;
			}
			in.close();

		} catch (Exception e) {
			e.printStackTrace();
			logger.error("Error At Line " + errorLine);
		}
		return true;
	}

	public int[] mapLine(String[] sLine, int lineNubmer) {
		int[] result = new int[sLine.length];
		for (int i = 0; i < result.length; i++) {
			result[i] = imaps.get(i).mapValue(sLine[i], lineNubmer);
		}
		return result;
	}

	/**
	 * save the maped integer valuse to outfile arff file
	 * 
	 * @param outFile
	 */
	public void applyLineFilter(String outFile) {
		try {
			BufferedWriter out = new BufferedWriter(new FileWriter(outFile));
			out.write("@relation \n");

			for (int i = 0; i < attributes.size(); i++) {
				StringBuffer att = new StringBuffer("@attribute "
						+ attributes.get(i) + " {");
				att.append(Tools.join(imaps.get(i).getIntegerSet(), ","));
				att.append("}\n");
				out.write(att.toString());
			}

			out.write("@data\n");
			for (int[] line : dataMap.values()) {
				out.write(Tools.join(line, ",") + "\n");
			}

			out.close();
		} catch (Exception e) {
			e.printStackTrace();
		}
	}

	public int[] getRow(int line) {
		return dataMap.get(line);
	}

	public int get(int col, int line) {
		return dataMap.get(line)[col];
	}

	public int get(int line) {
		// return dataMap.get(line)[attributes.size()-1];
		return get(attributes.size() - 1, line);
	}

	// public String getString(int col, int line){
	// return mapAsString(col,get(col,line));
	// }
	//	
	public String mapAsString(int col, int item) {
		return imaps.get(col).getString(item);
	}

	public String mapAsString(int item) {
		// return imaps.get(attributes.size()-1).getString(i);
		return mapAsString(attributes.size() - 1, item);
	}

	// TODO need testing
	public int[] mapRow(String[] row) {
		int[] result = new int[row.length];
		for (int i = 0; i < result.length; i++) {
			result[i] = imaps.get(i).get(row[i]);
		}
		return result;
	}

	// TODO need testing
	public String[] mapRowAsString(int[] ia) {
		String[] result = new String[ia.length];
		for (int i = 0; i < result.length; i++) {
			result[i] = imaps.get(i).getString(ia[i]);
		}
		return result;
	}

	public int size() {
		return dataMap.size();
	}

	/**
	 * 
	 * @return number of attributes including the class attribute
	 */
	public int getNumberOfColumns() {
		return attributes.size();
	}

	public Set<Integer> getLines() {
		return dataMap.keySet();
	}

	@Override
	public String toString() {
		StringBuffer result = new StringBuffer("attributes: "
				+ Tools.join(attributes, " "));

		result.append("\ndata:");
		for (Map.Entry<Integer, int[]> e : dataMap.entrySet())
			result.append("\n" + e.getKey() + "\t"
					+ Tools.join(e.getValue(), ","));

		return result.toString();
	}

	public String toString(List<Integer> predicted) {
		StringBuffer result = new StringBuffer("attributes: "
				+ Tools.join(attributes, " "));

		result.append("\ndata:");
		for (Map.Entry<Integer, int[]> e : dataMap.entrySet())
			result.append("\n" + e.getKey() + "\t"
					+ Tools.join(e.getValue(), ",") + "\t"
					+ predicted.get(e.getKey() - 1));

		return result.toString();
	}

	public static int orgColumn(long columnId) {
		long tmp = 0L;
		int bitCount = Long.bitCount(columnId);
		if (bitCount != 1)
			return -1;
		for (int i = 0, cols = 0; cols < bitCount && i < Long.SIZE; i++) {
			tmp = (1L << i);
			if ((tmp & columnId) != 0) {
				return i;
			}
		}
		return -1;
	}

	public static int[] bitsLocations(long columnId) {
		long tmp = 0L;
		int size = Long.bitCount(columnId);
		int[] index = new int[size];

		for (int i = 0, cols = 0; cols < size && i <= Long.SIZE; i++) {
			tmp = (1L << i);
			if ((tmp & columnId) != 0) {
				index[cols++] = i;
			}
		}
		return index;
	}

	public static int[] bitsLocations(BigInteger columnId) {
		int[] result = new int[columnId.bitCount()];
		int count = 0;
		for (int i = 0; i < columnId.bitLength(); i++) {
			if (columnId.testBit(i))
				result[count++] = i;
		}
		return result;
	}

	// return array of int values of the condidtions
	public Integer[] getCondition(long columnId, Integer rowId) {
		int[] row = dataMap.get(rowId);
		int[] bits = Data.bitsLocations(columnId);
		Integer[] result = new Integer[bits.length];
		for (int i = 0; i < result.length; i++) {
			result[i] = row[bits[i]];
		}
		return result;
	}

	public String[] getCondidtionAsString(long columnId, Integer rowId) {
		int[] bits = Data.bitsLocations(columnId);
		int[] row = dataMap.get(rowId);
		String[] result = new String[bits.length];
		for (int i = 0; i < result.length; i++) {
			result[i] = imaps.get(bits[i]).getString(row[bits[i]]);
			// result[i]= getString(bits[i], rowId);
		}
		return result;
	}

	public static int orgCol(long columnId) {
		int[] bits = Data.bitsLocations(columnId);
		if (bits.length == 1)
			return bits[0];
		else
			return -1;
	}

	public static int orgCol(BigInteger columnId) {
		int[] bits = Data.bitsLocations(columnId);
		if (bits.length == 1)
			return bits[0];
		else
			return -1;
		//		
		// for (int i = 0; i < columnId.bitLength(); i++) {
		// if(columnId.testBit(i))return i;
		// }
		// return -1;
	}

	public static StringBuffer join(String[] c, String sep) {
		StringBuffer result = new StringBuffer("");
		if (c == null || c.length == 0)
			return result;
		result.append(c[0]);
		for (int i = 1; i < c.length; i++) {
			result.append(sep + c[i]);
		}
		return result;

	}

	public static void applyLineFilter(String inFile, String outFile) {
		new Data(inFile).applyLineFilter(outFile);
	}

	public List<Integer> predictOne(Classifier classifier) {
		List<Integer> result = new ArrayList<Integer>();
		for (int[] line : dataMap.values()) {
			result.add(classifier.predictOne(line));
		}
		return result;
	}

	public static void main(String[] args) {
		// TODO Auto-generated method stub
		Data.applyLineFilter("data/iris.arff", "data/01.arff");
	}
}
