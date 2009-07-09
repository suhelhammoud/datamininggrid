package dm;

import java.util.*;
import java.math.BigInteger;
import org.apache.log4j.*;

import others.Tools;

public class Column {
	static Logger logger = Logger.getLogger(Column.class);

	public final long columnId;
	protected Map<Integer, Sccl> items;

	
	public Map<Integer, Sccl> getItems() {
		return items;
	}

	// public Column(BigInteger columnId) {
	// this.columnId=columnId.add(BigInteger.ZERO);
	// }
	public Column(long columnId) {
		this.columnId = columnId;
	}

	public static long second(long bi) {
		int[] bits = Data.bitsLocations(bi);
		if (bits.length < 2)
			return 0;
		return bi - Long.lowestOneBit(bi);
	}

	public static long first(long bi) {
		int[] bits = Data.bitsLocations(bi);
		if (bits.length < 2)
			return 0;
		return bi - Long.highestOneBit(bi);
	}

	// clear the most right bit
	public static BigInteger firstSubColumn(BigInteger bi) {
		int[] bits = Data.bitsLocations(bi);
		if (bits.length < 2)
			return BigInteger.ZERO;
		return bi.clearBit(bits[0]);
		//		
		// for (int i = 0; i < bi.bitLength(); i++)
		// if(bi.testBit(i))
		// return bi.clearBit(i);
		// return BigInteger.ZERO;
	}

	// clear the most left bit
	public static BigInteger secondSubColumn(BigInteger bi) {
		int[] bits = Data.bitsLocations(bi);
		if (bits.length < 2)
			return BigInteger.ZERO;
		return bi.clearBit(bits[bits.length - 1]);
		// for (int i = bi.bitLength()-1; i >= 0; i--)
		// if(bi.testBit(i))
		// return bi.clearBit(i);
		// return BigInteger.ZERO;
	}

	/**
	 * DataMin.addCandidateRule You must call generateForSupport(double support)
	 * at least once before you call this function
	 */
	public List<Sccl> generateForConfidence(double conf) {
		List<Sccl> result = new ArrayList<Sccl>();
		for (Sccl sccl : items.values()) {
			if (sccl.getConfidence() < conf)
				continue;
			result.add(sccl);
		}
		return result;
	}

	/**
	 * 
	 */
	public void calcSupportsAndConfidences() {
		for (Sccl sccl : items.values()) {
			sccl.getSupport();
			sccl.getConfidence();
		}

	}

	/**
	 * Read the data from the Data instance, generate atomic occurances
	 * 
	 * @param availableLines
	 * @param data
	 *            TODO
	 */
	public void generateAtomicOccurances(Set<Integer> availableLines, Data data) {
		// if(columnId.equals(BigInteger.valueOf(8))){
		// System.out.println("start debugging");
		// }
		items = new TreeMap<Integer, Sccl>();
		// scan the entity and get the distinct values and the lines which
		// accompany each distinct value
		int col = Data.orgCol(columnId);
		for (Integer line : availableLines) {

			// TODO: tobe removed soon

			int ruleLine = data.get(col, line);
			int cls = data.get(line);

			if (items.keySet().contains(ruleLine)) {
				Sccl sccl = items.get(ruleLine);
				if (sccl == null) {
					logger.error("item: " + col + "," + line + " has no sccl");
				}

				sccl.addLine(line, cls);
				// logger.info("add line to label "+ line+" ,"+cls);
			} else {
				// TODO all data are line mapped before working
				// Sccl sccl=new Sccl(columnId,line);
				Sccl sccl = new Sccl(columnId, ruleLine);
				sccl.addLine(line, cls);
				// logger.info("add line to label "+ line+" ,"+cls);

				items.put(line, sccl);
			}
		}

	}

	public boolean generateForSupport(int oSupport, Column fColumn,
			Column sColumn) {
		items = new TreeMap<Integer, Sccl>();
		boolean result = false;
		Collection<int[]> possibleEntity = Column.product(fColumn.items
				.keySet(), sColumn.items.keySet());
		for (int[] ia : possibleEntity) {
			Sccl tsccl = new Sccl(fColumn.getSccl(ia[0]));
			Sccl tsccl2 = new Sccl(sColumn.getSccl(ia[1]));
			Sccl sccl = Sccl.compose(columnId, tsccl, tsccl2);
			if (sccl.size() > 0 && sccl.getSupport() >= oSupport) {
				items.put(sccl.getRowId(), sccl);
				result = true;
			}
		}
		return result;
	}

	public static Collection<int[]> product(Collection<Integer> set1,
			Collection<Integer> set2) {
		Set<int[]> result = new HashSet<int[]>(set1.size() * set2.size());
		for (Integer iter1 : set1) {
			for (Integer iter2 : set2) {
				int[] itm = new int[] { iter1, iter2 };
				result.add(itm);
			}
		}
		return result;
	}

	/**
	 * Generate atomic values
	 * 
	 * @param oSupp
	 *            : support
	 * @param availableLines
	 * @param data
	 *            TODO
	 * @return true if any item survived the support
	 */
	public boolean generateAtomicForSupport(int oSupp,
			Set<Integer> availableLines, Data data) {
		if (Long.bitCount(columnId) != 1) {
			logger.error("column is no atomic");
			return false;
		}

		generateAtomicOccurances(availableLines, data);

		Iterator<Integer> iter = items.keySet().iterator();
		while (iter.hasNext()) {
			Integer line = iter.next();
			Sccl sccl = items.get(line);
			if (sccl.getSupport() < oSupp) {
				iter.remove();
				continue;
			}
		}
		if (items.size() == 0)
			return false;
		return true;
	}

	public Sccl getSccl(Integer i) {
		return items.get(i);
	}

	@Override
	public String toString() {
		StringBuffer result = new StringBuffer("Column id=" + columnId
				+ ", size=" + size() + ",(" + first(columnId) + ","
				+ second(columnId) + ")");
		if (items != null)
			for (Map.Entry<Integer, Sccl> i : items.entrySet()) {
				result
						.append("\n" + Tools.join(Data.bitsLocations(columnId), ",")+"-"+ i.getKey() + "\t:"
								+ i.getValue().toString());
			}
		result.append("\n");
		return result.toString();
	}

	public int size() {
		return items.size();
	}

	public static void main(String[] args) {
		// test
		Data data = new Data();
		data.readFromFile("data/1.arff");
		System.out.println(data);
	}

}