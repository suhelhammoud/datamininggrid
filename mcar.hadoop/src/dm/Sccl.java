package dm;

//TODO make this class serilaizable 

import hdm.HColumn;
import hdm.HRuleRank;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInput;
import java.io.DataInputStream;
import java.io.DataOutput;
import java.io.DataOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.ObjectInputStream;
import java.io.ObjectOutput;
import java.io.ObjectOutputStream;
import java.io.Serializable;
import java.math.BigInteger;
import java.nio.ByteBuffer;
import java.text.DecimalFormat;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;
import java.util.TreeSet;
import java.util.Map.Entry;

import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.mapred.RecordWriter;
import org.apache.log4j.Logger;

import others.Tools;

public class Sccl implements WritableComparable<Object> {
	static Logger logger = Logger.getLogger(Sccl.class);
	static DecimalFormat format = new DecimalFormat("00.00000");

	public long columnId;
	private int rowId = Integer.MAX_VALUE;
	/**
	 * mapLines: class -> set(lines)
	 */
	private Map<Integer, Set<Integer>> mapLines;
	private int oSupport = -1;
	private double confidence = -1;
	private int allOcc;

	private volatile int hashCode = 0;

	public Sccl() {
		this(-1);
	}

	public Sccl(long columnId) {
		this.columnId = columnId;
		mapLines = new TreeMap<Integer, Set<Integer>>();
		allOcc = 0;
	}

	// public Sccl(long longColumnId){
	// this(BigInteger.valueOf(longColumnId));
	// }

	public Sccl(long columnId, Integer rowId) {
		this(columnId);
		this.rowId = rowId;
	}

	public Sccl(long columnId, int rowId, Integer cid, TreeSet<Integer> ts) {
		this(columnId);
		this.rowId = rowId;
		mapLines.put(cid, new HashSet<Integer>(ts));
		allOcc += ts.size();
	}

	public Sccl(Sccl tsccl) {
		this(tsccl.columnId);
		this.rowId = tsccl.rowId;
		allOcc = tsccl.allOcc;
		oSupport = tsccl.oSupport;
		confidence = tsccl.confidence;

		for (Map.Entry<Integer, Set<Integer>> iter : tsccl.mapLines.entrySet()) {
			this.mapLines.put(iter.getKey(), new HashSet<Integer>(iter
					.getValue()));
		}
	}

	public static Sccl compose(long columnid, Sccl s1, Sccl s2) {
		Sccl result = new Sccl(columnid);
		// this.columnId= s1.columnId;

		if (s1.columnId == s2.columnId) {
			logger.error(s1.columnId + " ==" + s2.columnId);
			return result;
		}
		if (s1.mapLines.size() == 0 || s2.mapLines.size() == 0) {
			logger.error("s1 or s2 map size ==0");
			return result;
		}

		// TODO: change TreeSet to HashSet for the performance
		Set<Integer> set = new TreeSet<Integer>();

		set.addAll(s1.mapLines.keySet());
		set.retainAll(s2.mapLines.keySet());

		if (set.size() == 0)
			return result;
		for (Integer cls : set) {
			Set<Integer> value = new HashSet<Integer>(s1.mapLines.get(cls));
			value.retainAll(s2.mapLines.get(cls));
			if (value.size() == 0) {
				// logger.info("cls "+ cls+ " has no occorances in "+columnId);
				continue;
			}
			result.mapLines.put(cls, value);
			result.allOcc += value.size();
		}
		// result.rowId=minimumLine();
		return result;
	}

	public int minimumLine() {
		int result = Integer.MAX_VALUE;
		for (Set<Integer> set : mapLines.values())
			for (Integer i : set)
				if (i < result)
					result = i;
		return result;
	}

	/**
	 * + mapLines, allOcc, labelId,
	 * 
	 * @param line
	 * @param label
	 * @return true if the line is added
	 */
	public boolean addLine(Integer line, Integer cls) {
		Set<Integer> ts = mapLines.get(cls);

		if (ts == null) {
			ts = new TreeSet<Integer>();
			mapLines.put(cls, ts);
		}
		if (!ts.add(line))
			return false;
		allOcc++;
		hashCode = 0;
		return true;
	}

	public int compareTo(Object o) {

		Sccl s = (Sccl) o;
		// compare confidences;
		double doubleDif = getConfidence() - s.getConfidence();
		if (Math.abs(doubleDif) > 0.000000001)
			return -(int) Math.signum(doubleDif);

		// compare support
		int dif = getSupport() - s.getSupport();
		if (dif != 0)
			return -dif;

		// compare how many atomic column
		dif = Long.bitCount(columnId) - Long.bitCount(s.columnId);
		if (dif != 0)
			return dif;

		// compare allOcc
		dif = allOcc - s.allOcc;
		if (dif != 0)
			return -dif;

		// compare columns name
		dif = (int) (columnId - s.columnId);
		if (dif != 0)
			return dif;

		// compare rowIds
		dif = rowId - s.rowId;
		if (dif != 0)
			return -dif;

		// TODO: check this later
		// logger.error("two equal ranked rules");

		return 0;
	}

	@Override
	public boolean equals(Object obj) {
		if (obj == this)
			return true;
		if (!(obj instanceof Sccl))
			return false;
		Sccl o = (Sccl) obj;
		// TODO may add "labelId.equals(o.labelId) &&" later
		return getColumnId() == o.getColumnId() && getRowId() == getRowId()
				&& mapLines.equals(o.mapLines);
	}

	private Set<Integer> getOtherLines(Integer line) {
		Set<Integer> result = new HashSet<Integer>();
		for (Integer i : mapLines.keySet()) {
			if (i.equals(line))
				continue;
			result.addAll(mapLines.get(i));
		}
		return result;
	}

	public Set<Integer> getLines(Integer line) {
		return mapLines.get(line);
	}

	public Set<Integer> getAllLines() {
		Set<Integer> result = new HashSet<Integer>(allOcc);
		for (Set<Integer> set : mapLines.values()) {
			result.addAll(set);
		}
		// to check the work of the algorithm
		if (result.size() != allOcc)
			logger.error("allOcc=" + allOcc + " != mapLines.allLines.size="
					+ result.size());
		return result;
	}

	public long getColumnId() {
		return columnId;
	}

	public final double getConfidence() {
		if (confidence >= 0)
			return confidence;
		// TODO optimise it later
		confidence = (double) getSupport() / (double) allOcc;
		return confidence;
	}

	/**
	 * get the first label
	 * 
	 * @return
	 */
	public Entry<Integer, Set<Integer>> mainItem() {
		Entry<Integer, Set<Integer>> result = null;
		int max = Integer.MIN_VALUE;
		for (Entry<Integer, Set<Integer>> e : mapLines.entrySet()) {
			if (e.getValue().size() > max) {
				max = e.getValue().size();
				result = e;
			}
		}
		return result;
	}

	public Set<Integer> get(Integer label) {
		return mapLines.get(label);
	}

	/**
	 * 
	 * @return lines of the labels other than the defult label
	 */

	// line number of the first occurance
	// TODO it is not necessary to be the first line of the main label
	public int getRowId() {
		if (rowId == Integer.MAX_VALUE) {
			rowId = minimumLine();
		}
		return rowId;
	}

	public HRuleRank hRuleRank() {
		return new HRuleRank(columnId, getRowId(), getConfidence(),
				getSupport(), size());
	}

	public final int getSupport() {
		if (oSupport >= 0)
			return oSupport;
		oSupport = mainItem().getValue().size();
		return oSupport;
	}

	/**
	 * two sccl are equal when columnId and rowId are equal
	 */
	@Override
	public int hashCode() {
		// Lazily initialized, cached hashCode
		if (hashCode == 0) {
			int result = (int) (columnId ^ (columnId >>> 32));// from longCode
			result = 37 * result + rowId;
			hashCode = result;
		}
		return hashCode;
	}

	public Integer getKey(Integer line) {
		for (Map.Entry<Integer, Set<Integer>> iter : mapLines.entrySet()) {
			if (iter.getValue().contains(line))
				return iter.getKey();
		}
		return -1;
	}

	public boolean isCovering(Integer rid) {
		for (Set<Integer> iter : mapLines.values()) {
			if (iter.contains(rid))
				return true;
		}
		return false;
	}

	/**
	 * 
	 * @param
	 * @return label of the lines coverd with the lines
	 */
	public IntLabelLines cover(Set<Integer> availableLines,
			Set<Integer> taggedLines) {
		int maxSize = 0;
		int maxLabel = 0;
		Set<Integer> maxSet = new TreeSet<Integer>();// TODO change to HashSet
														// later
		for (Entry<Integer, Set<Integer>> entry : mapLines.entrySet()) {

			Set<Integer> tempSet = new TreeSet<Integer>();// TODO change to
															// HashSet later
			for (Integer line : entry.getValue()) {
				if (availableLines.contains(line))
					continue;
				tempSet.add(line);
			}
			if (tempSet.size() > maxSize) {
				maxSet = tempSet;
				maxSize = maxSet.size();
				maxLabel = entry.getKey();
			}
		}

		return new IntLabelLines(maxLabel, maxSet);
	}

	public CheckResult check(Set<Integer> taggedLines, Set<Integer> allLines) {

		int maxSize = 0;
		int maxLabel = 0;
		// trueCovered and falseCovered
		Set<Integer> trueCovered = new TreeSet<Integer>();// TODO change to
															// HashSet later
		Set<Integer> falseCovered = new TreeSet<Integer>();// TODO change to
															// HashSet later
		Set<Integer> notCovered = new TreeSet<Integer>();// TODO change to
															// HashSet later

		for (Entry<Integer, Set<Integer>> entry : mapLines.entrySet()) {

			Set<Integer> tCovered = new TreeSet<Integer>();// TODO change to
															// HashSet later
			// Set<Integer> fCovered= new TreeSet<Integer>();//TODO change to
			// HashSet later
			Set<Integer> nCovered = new TreeSet<Integer>();// TODO change to
															// HashSet later

			for (Integer line : entry.getValue()) {
				if (!allLines.contains(line))
					continue;
				if (taggedLines.contains(line)) {
					// falseCovered.add(line);
					nCovered.add(line);
				} else {
					tCovered.add(line);
				}
			}
			if (tCovered.size() >= maxSize) {
				falseCovered.addAll(trueCovered);
				trueCovered = tCovered;
				maxSize = trueCovered.size();
				maxLabel = entry.getKey();
			} else if (tCovered.size() < maxSize) {
				falseCovered.addAll(tCovered);
				notCovered.addAll(nCovered);

			} else // tCovered.size()== maxSize
			// TODO try to test this later without the last condition, make it
			// random
			if (mapLines.get(maxLabel) != null
					&& entry.getValue().size() > mapLines.get(maxLabel).size()) {

				falseCovered.addAll(trueCovered);
				trueCovered = tCovered;
				maxSize = trueCovered.size();
				maxLabel = entry.getKey();
			} else {
				falseCovered.addAll(tCovered);
				notCovered.addAll(nCovered);
			}
		}

		int linesSize = trueCovered.size() + falseCovered.size()
				+ notCovered.size();
		// debug error
		// TODO whole section to be removed later
		if (linesSize != allOcc && false) {
			int trueSize = 0;
			Set<Integer> temp = new TreeSet<Integer>(taggedLines);

			for (Set<Integer> set : mapLines.values()) {
				trueSize += set.size();
				temp.addAll(set);
			}
			temp.removeAll(taggedLines);

			logger.error(" TrueSize=" + trueSize + " size " + linesSize
					+ " != allOcc " + allOcc + "\n FalseCovered "
					+ falseCovered + "\n TrueCovered " + trueCovered
					+ "\n Set=" + temp + "\n Sccl " + toString());
		}
		return new CheckResult(maxLabel, trueCovered, falseCovered, notCovered);
	}

	public CheckResult check(Set<Integer> taggedLines) {
		// TODO to delete the debug code later
		if (columnId == 8 && getRowId() == 71)
			System.out.println("start debugging");
		int maxSize = 0;
		int maxLabel = 0;
		// trueCovered and falseCovered
		Set<Integer> trueCovered = new TreeSet<Integer>();// TODO change to
															// HashSet later
		Set<Integer> falseCovered = new TreeSet<Integer>();// TODO change to
															// HashSet later
		for (Entry<Integer, Set<Integer>> entry : mapLines.entrySet()) {

			Set<Integer> tCovered = new TreeSet<Integer>();// TODO change to
															// HashSet later
			// Set<Integer> fCovered= new TreeSet<Integer>();//TODO change to
			// HashSet later

			// TODO remove for the performance

			for (Integer line : entry.getValue()) {
				if (taggedLines.contains(line)) {
					falseCovered.add(line);
					// fCovered.add(line);
				} else {
					tCovered.add(line);
				}
			}
			if (tCovered.size() >= maxSize) {
				falseCovered.addAll(trueCovered);
				trueCovered = tCovered;
				// falseCovered=fCovered;
				maxSize = trueCovered.size();
				maxLabel = entry.getKey();
			} else if (tCovered.size() < maxSize)
				falseCovered.addAll(tCovered);
			else // tCovered.size()== maxSize
			// TODO try to test this later without the last condition, make it
			// random
			if (mapLines.get(maxLabel) != null
					&& entry.getValue().size() > mapLines.get(maxLabel).size()) {
				falseCovered.addAll(trueCovered);
				trueCovered = tCovered;
				maxSize = trueCovered.size();
				maxLabel = entry.getKey();
			} else
				falseCovered.addAll(tCovered);
		}

		int linesSize = trueCovered.size() + falseCovered.size();
		if (linesSize != allOcc) {
			int trueSize = 0;
			Set<Integer> temp = new TreeSet<Integer>(taggedLines);

			for (Set<Integer> set : mapLines.values()) {
				trueSize += set.size();
				temp.addAll(set);
			}
			temp.removeAll(taggedLines);

			logger.error(" TrueSize=" + trueSize + " size " + linesSize
					+ " != allOcc " + allOcc + "\n FalseCovered "
					+ falseCovered + "\n TrueCovered " + trueCovered
					+ "\n Set=" + temp + "\n Sccl " + toString());
		}
		return new CheckResult(maxLabel, trueCovered, falseCovered);
	}

	@Override
	public String toString() {
		StringBuffer sb = new StringBuffer("(" + columnId + " , " + rowId
				+ ")" +"\tocc:" + allOcc + "\tsup:" + oSupport + ", \tconf:"
				+ format.format(confidence) + ",\t"
				+ "\n\t\t");
		
		for (Map.Entry<Integer, Set<Integer>> iter : mapLines.entrySet()) {
			List<Integer> sorted=new ArrayList<Integer>(iter.getValue());
			Collections.sort(sorted);
			sb.append(iter.getKey()+":"+sorted + " , " );
			
		}
		return sb.toString();
	}

	public int size() {
		return allOcc;
	}

	private String[] fillRule(Data data) {
		String[] result = new String[data.getNumberOfColumns() - 1];
		for (int i = 0; i < result.length; i++) {
			result[i] = "*";
		}
		String[] arrs = data.getCondidtionAsString(columnId, rowId);
		int[] bits = Data.bitsLocations(columnId);
		for (int i = 0; i < bits.length; i++) {
			result[bits[i]] = arrs[i];
		}
		return result;
	}

	public String printAsRule(Data data) {
		String[] cond = fillRule(data);
		Entry<Integer, Set<Integer>> mainItem = mainItem();
		Integer key = mainItem == null ? 0 : mainItem().getKey();
		return Tools.join(cond, "\t") + "\t" + key + "\tocc:" + allOcc
				+ "\tsup:" + oSupport + ", \tconf:" + format.format(confidence);
	}

	public List<LabelFrequency> getLebFreq(Data data) {
		List<LabelFrequency> result = new ArrayList<LabelFrequency>();
		for (Entry<Integer, Set<Integer>> entry : mapLines.entrySet()) {
			String label = data.mapAsString(entry.getKey());
			int freq = entry.getValue().size();
			result.add(new LabelFrequency(label, freq, entry.getKey()));
		}
		return result;
	}

	public ScclRule getScclRule() {
		return new ScclRule(columnId, getRowId());
	}

	private boolean addSet(int label, Set<Integer> lines) {
		boolean result = false;
		if (lines == null || lines.size() == 0)
			return false;

		hashCode = 0;

		Set<Integer> set = mapLines.get(label);
		if (set == null) {
			mapLines.put(label, lines);
			allOcc += lines.size();
			return true;
		}

		for (Integer i : lines) {
			if (set.add(i)) {
				allOcc++;
				result = true;
			}
		}

		return result;
	}

	public static Map<Integer, Set<Integer>> readMap(DataInput in)
			throws IOException {
		int size = in.readInt();
		Map<Integer, Set<Integer>> result = new HashMap<Integer, Set<Integer>>(
				size);
		for (int i = 0; i < size; i++) {
			result.put(in.readInt(), readSet(in));
		}
		return result;
	}

	public static void writeMap(Map<Integer, Set<Integer>> map, DataOutput out)
			throws IOException {
		out.writeInt(map.size());
		for (Entry<Integer, Set<Integer>> entry : map.entrySet()) {
			out.writeInt(entry.getKey());
			writeSet(entry.getValue(), out);
		}
	}

	public static Set<Integer> readSet(DataInput in) throws IOException {
		int size = in.readInt();
		Set<Integer> result = new HashSet<Integer>(size);
		for (int i = 0; i < size; i++) {
			result.add(in.readInt());
		}
		return result;
	}

	public static void writeSet(Set<Integer> set, DataOutput out)
			throws IOException {
		out.writeInt(set.size());
		for (Integer item : set) {
			out.writeInt(item);
		}
	}

	public byte[] toBytes_old() throws IOException {
		// Serialize to a byte array
		ByteArrayOutputStream bos = new ByteArrayOutputStream();
		DataOutput out = new DataOutputStream(bos);
		this.write(out);
		bos.close();
		// Get the bytes of the serialized object
		// byte[] buf = bos.toByteArray();
		return bos.toByteArray();
	}

	public byte[] toBytes() {
		// Serialize to a byte array
		int length = Long.SIZE + Integer.SIZE
				* (mapLines.size() * 2 + allOcc + 2);
		length = length / Byte.SIZE;

		ByteBuffer bb = ByteBuffer.allocate(length);
		bb.putLong(columnId);
		bb.putInt(rowId);

		bb.putInt(mapLines.size());
		for (Entry<Integer, Set<Integer>> e : mapLines.entrySet()) {
			bb.putInt(e.getKey());
			bb.putInt(e.getValue().size());
			for (Integer line : e.getValue()) {
				bb.putInt(line);
			}

		}
		return bb.array();
	}

	public static Sccl from(byte[] bytes) {
		ByteBuffer bb = ByteBuffer.wrap(bytes);
		Sccl sccl = new Sccl(bb.getLong(), bb.getInt());

		int mapSize = bb.getInt();
		for (int i = 0; i < mapSize; i++) {
			int label = bb.getInt();
			int setSize = bb.getInt();
			Set<Integer> lines = new TreeSet<Integer>();
			for (int j = 0; j < setSize; j++) {
				lines.add(bb.getInt());
			}
			sccl.addSet(label, lines);
		}
		return sccl;
	}

	public static Sccl fromBytes(byte[] bytes) throws IOException,
			ClassNotFoundException {
		DataInput in = new DataInputStream(new ByteArrayInputStream(bytes));
		Sccl result = new Sccl();
		result.readFields(in);
		return result;
	}

	@Override
	public void readFields(DataInput in) throws IOException {
		mapLines.clear();
		columnId = in.readLong();
		rowId = in.readInt();
		mapLines = readMap(in);

	}

	@Override
	public void write(DataOutput out) throws IOException {
		out.writeLong(columnId);
		out.writeInt(rowId);
		writeMap(mapLines, out);
	}

}
