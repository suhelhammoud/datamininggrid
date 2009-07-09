package dm;

import java.math.BigInteger;
import java.util.ArrayList;
import java.util.List;

import org.apache.log4j.Logger;

public class RuleID implements Comparable<RuleID> {
	/**
	 * Logger for this class
	 */
	private static final Logger logger = Logger.getLogger(RuleID.class);

	public final BigInteger columnId;
	public final int rowId;

	public RuleID(RuleID rid) {
		this(rid.columnId, rid.rowId);
	}

	public RuleID(BigInteger columnId, int rowId) {
		this.columnId = columnId;
		this.rowId = rowId;
	}

	public List<Integer> getItems(Data data) {
		List<Integer> result = new ArrayList<Integer>();
		return result;
	}

	private int hashCode = 0;

	public int compareTo(RuleID o) {
		// TODO Auto-generated method stub
		int dif = columnId.compareTo(o.columnId);
		if (dif == 0)
			return rowId - o.rowId;
		return 0;
	}

	@Override
	public boolean equals(Object obj) {
		RuleID o = (RuleID) obj;
		return columnId.equals(o.columnId) && (rowId == o.rowId);
	}

	@Override
	public int hashCode() {
		if (hashCode == 0) {
			hashCode = 37 * columnId.hashCode() + rowId;
		}
		return hashCode;
	}

}
