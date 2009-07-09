package dm;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.math.BigInteger;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Set;

import org.apache.log4j.Logger;

import others.Tools;

public class ScclRule {
	static Logger logger = Logger.getLogger(ScclRule.class);
	public static int numOfAttributes;

	public final long columnId;
	public final int rowId;

	String[] condition;
	Integer[] intCondidtion;
	List<LabelFrequency> lebFreq;

	public ScclRule(long columnId, int rowId) {
		this.columnId = columnId;
		this.rowId = rowId;
		lebFreq = new ArrayList<LabelFrequency>();
	}

	public ScclRule(Sccl sccl) {
		this(sccl.columnId, sccl.getRowId());
	}

	public void addLabelFreq(int freq, int intLabel) {
		lebFreq.add(new LabelFrequency(freq, intLabel));
	}

	public void fill(Data data) {
		for (LabelFrequency lf : lebFreq) {
			lf.setLabel(data.mapAsString(lf.intLabel));
		}
		condition = data.getCondidtionAsString(columnId, rowId);
		intCondidtion = data.getCondition(columnId, rowId);
		if (condition.length == 0 || intCondidtion.length == 0) {
			logger.error("condition length =0");
		}
		Collections.sort(lebFreq);
		ScclRule.numOfAttributes = data.getNumberOfColumns();

	}

	public boolean accept(int[] line) {

		if (line == null)
			return false;
		if (line.length == 0)
			return false;
		if (line.length < intCondidtion.length) {
			logger.error("line.length=" + line.length
					+ " != intCondidtion.size=" + intCondidtion.length);
			return false;
		}
		List<Integer> lineList = new ArrayList<Integer>(line.length);
		for (int i : line) {
			lineList.add(i);
		}
		return accept(lineList);

	}

	public boolean accept(List<Integer> line) {
		if (line == null)
			return false;
		if (line.size() == 0)
			return false;
		if (line.size() < intCondidtion.length) {
			logger.error("line.size=" + line.size() + " != intCondidtion.size="
					+ intCondidtion.length);
			return false;
		}
		int[] bits = Data.bitsLocations(columnId);
		for (int i = 0; i < bits.length; i++) {
			if (!intCondidtion[i].equals(line.get(bits[i])))
				return false;
		}
		return true;
	}

	public List<LabelFrequency> get() {
		return new ArrayList<LabelFrequency>(lebFreq);
	}

	public LabelFrequency getOne() {
		if (lebFreq.size() > 0)
			return lebFreq.get(0);
		else
			return new LabelFrequency(-1, -1);
	}

	public boolean accept(List<String> line, boolean b) {
		if (line == null)
			return false;
		if (line.size() == 0)
			return false;
		if (line.size() < condition.length) {
			logger.error("line.size=" + line.size() + " != condistion.size="
					+ condition.length);
			return false;
		}
		int[] bits = Data.bitsLocations(columnId);
		for (int i : bits) {
			if (!condition[i].equals(line.get(i)))
				return false;
		}
		return true;
	}

	@Override
	public String toString() {
		StringBuffer result = new StringBuffer();
		String[] cond = new String[numOfAttributes - 1];
		for (int i = 0; i < cond.length; i++) {
			cond[i] = "*";
		}
		int[] bits = Data.bitsLocations(columnId);
		for (int i = 0; i < condition.length; i++) {
			cond[bits[i]] = condition[i];
		}
		result.append(Tools.join(cond, "\t"));

		int allOcc = 0;
		for (LabelFrequency lf : lebFreq) {
			allOcc += lf.freq;
		}
		result.append("\t" + allOcc + "\t" + lebFreq);

		return result.toString();
	}

	public static void main(String[] args) {
		ScclRule.numOfAttributes = 8;
		ScclRule scclRule = new ScclRule(5, 1);
		scclRule.intCondidtion = new Integer[] { 1, 3 };
		scclRule.condition = new String[] { "1", "3" };
		int[] line = new int[] { 1, 4, 33, 4, 5 };
		boolean b = scclRule.accept(line);
		logger.info(b + "\t" + scclRule.toString());
		System.out.println("done");

	}
}
