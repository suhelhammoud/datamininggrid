package hdm;

import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.MasterNotRunningException;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Scanner;
import org.apache.hadoop.hbase.io.BatchUpdate;
import org.apache.hadoop.hbase.io.Cell;
import org.apache.hadoop.hbase.io.RowResult;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.log4j.Logger;
import org.jruby.ast.SClassNode;

import others.Tools;

import com.sun.jndi.ldap.EntryChangeResponseControl;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;

import dm.Column;
import dm.Data;
import dm.Sccl;

public class HColumn extends Column {
	/**
	 * Logger for this class
	 */
	static final Logger logger = Logger.getLogger(HColumn.class);

	HBaseConfiguration conf;
	HTable hItems;
	// private static final String family=":f";
	private byte[] hName;
	private long linesSize = 0;

	// private byte[] startRow,stopRow;

	public HColumn(long columnId, HBaseConfiguration conf) throws IOException {
		super(columnId);
		this.conf = conf;
		hItems = new HTable(conf, Bytes.toBytes(Long.bitCount(columnId)));
		// test it
		hItems.setAutoFlush(true);
		// hName=(HUtil.FAMILY;
	}

	public byte[] startRow() {
		return HRow.toBytes(columnId, 1);
	}

	public byte[] stopRow() {
		return HRow.toBytes(columnId, Integer.MAX_VALUE);
	}

	public long addItem(Integer item, Sccl sccl) throws IOException {
		// BatchUpdate batchUpdate = new BatchUpdate(Bytes.toBytes(line));
		BatchUpdate batchUpdate = new BatchUpdate(HRow.toBytes(columnId, item));
		batchUpdate.put(hName, sccl.toBytes());
		// hItems.commit(batchUpdate);
		return ++linesSize;
	}

	public void create() throws IOException {
		hItems = HUtil.create(conf, Bytes.toBytes(Long.bitCount(columnId)));
	}

	public List<Integer> hGenerateForConfidence(HClassifier hClassifier,
			double confidence) throws IOException {
		List<Integer> result = new ArrayList<Integer>();
		Scanner s = hItems.getScanner(new byte[][] { hName }, startRow(),
				stopRow());
		try {
			for (RowResult r : s) {
				Integer key = Bytes.toInt(r.getRow());
				Cell cell = r.get(hName);
				if (cell == null) {
					logger.error("no sccl for Item key:" + key);
					return result;
				}
				Sccl sccl = Sccl.fromBytes(cell.getValue());
				if (sccl.getConfidence() < confidence)
					continue;
				//
				hClassifier.addHCandidateRule(sccl);
				result.add(key);
			}
		} catch (Exception e) {
			e.printStackTrace();
		} finally {
			s.close();
		}
		return result;
	}

	public boolean hGenerateAtomicForSupport(int supp,
			Set<Integer> availableLines, Data data) {

		super.generateAtomicForSupport(supp, availableLines, data);
		if (items.size() == 0)
			return false;

		try {
			create();
			for (Map.Entry<Integer, Sccl> e : items.entrySet()) {
				addItem(e.getKey(), e.getValue());
			}
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		items.clear();
		return true;
	}

	public boolean hGenerateForSupport(int support, HColumn column1,
			HColumn column2) throws IOException, ClassNotFoundException {
		boolean result = false;

		Scanner s1 = column1.hItems.getScanner(new byte[][] { column1.hName },
				column1.startRow(), column1.stopRow());
		Scanner s2 = column2.hItems.getScanner(new byte[][] { column2.hName },
				column2.startRow(), column2.stopRow());

		column1.items.clear();
		column2.items.clear();

		try {
			for (RowResult r1 : s1) {
				Integer key = Bytes.toInt(r1.getRow());
				Cell cell = r1.get(column1.hName);
				if (cell == null) {
					logger.error(" no Sccl for Item key:" + key);
					return false;
				}
				Sccl value = Sccl.fromBytes(cell.getValue());
				column1.items.put(key, value);

			}
		} finally {
			s1.close();
		}

		try {
			for (RowResult r2 : s2) {
				Integer key = Bytes.toInt(r2.getRow());
				Cell cell = r2.get(column2.hName);
				if (cell == null) {
					logger.error(" no Sccl for Item key:" + key);
					return false;
				}
				Sccl value = Sccl.fromBytes(cell.getValue());
				for (Sccl sccl1 : column1.items.values()) {
					Sccl sccl = Sccl.compose(columnId, sccl1, value);
					if (sccl.size() > 0 && sccl.getSupport() >= support) {
						addItem(sccl.getRowId(), sccl);
						result = true;
					}
					column1.items.clear();
				}
			}
		} finally {
			hItems.flushCommits();
			s2.close();
		}
		return result;
	}

	@Override
	public String toString() {
		StringBuffer result = new StringBuffer("Column id=" + columnId
				+ ", size=" + size() + ",(" + first(columnId) + ","
				+ second(columnId) + ")");
		if (items != null)
			for (Map.Entry<Integer, Sccl> i : items.entrySet()) {
				result
						.append("\n" + i.getKey() + ":"
								+ i.getValue().toString());
			}
		result.append("\n");
		// return result.toString();

		Scanner s = null;

		try {
			s = hItems.getScanner(new byte[][] { HUtil.FAMILY }, startRow(),
					stopRow());
			for (RowResult r : s) {
				int key = HRow.getRowId(r.getRow());
				byte[] ba = r.get(HUtil.FAMILY).getValue();
				Sccl sccl = Sccl.from(ba);
				result.append("\n" + key + ":" + sccl);

			}
		} catch (IOException e) {
			e.printStackTrace();
		} finally {
			s.close();
		}
		result.append("\n");
		return result.toString();

	}

}
