package pkg;

import org.apache.log4j.Logger;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.TableNotFoundException;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Scanner;
import org.apache.hadoop.hbase.io.BatchUpdate;
import org.apache.hadoop.hbase.io.Cell;
import org.apache.hadoop.hbase.io.RowResult;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.HConstants;

import dm.Data;

public class MCARInit {
	/**
	 * Logger for this class
	 */
	private static final Logger logger = Logger.getLogger(MCARInit.class);

	static String familyName = "familyName:";
	private static final HColumnDescriptor column = new HColumnDescriptor(
			familyName);

	private static final byte[] nosuchTable = Bytes.toBytes("nosuchTable");
	private static final byte[] tableAname = Bytes.toBytes("tableA");
	private static final byte[] tableBname = Bytes.toBytes("tableB");

	private static final byte[] row = Bytes.toBytes("row");

	private static final byte[] attrName = Bytes.toBytes("TESTATTR");
	private static final byte[] attrValue = Bytes.toBytes("somevalue");

	HBaseConfiguration conf = new HBaseConfiguration();

	HBaseAdmin admin;

	public void deleteAllTables() throws IOException {
		if (admin == null)
			admin = new HBaseAdmin(conf);

		HTableDescriptor[] tables = admin.listTables();
		for (HTableDescriptor desc : tables) {
			admin.disableTable(desc.getName());
			admin.deleteTable(desc.getName());
		}
	}

	public String createTable(byte[] tableName, String family)
			throws IOException {
		admin = new HBaseAdmin(conf);

		HTableDescriptor d_tdata = new HTableDescriptor(tableName);

		HColumnDescriptor cdesc = new HColumnDescriptor(family);
		cdesc.setMaxVersions(0);
		cdesc.setBlockCacheEnabled(true);
		d_tdata.addFamily(cdesc);

		if (admin.tableExists(d_tdata.getName())) {
			admin.disableTable(d_tdata.getName());
			admin.deleteTable(d_tdata.getName());

		}
		admin.createTable(d_tdata);
		logger.info("create table " + d_tdata.getNameAsString());
		return d_tdata.getNameAsString();
	}

	public List<String> createAllTables(int numOfTables) throws IOException {

		List<String> result = new ArrayList<String>();
		for (int i = 1; i <= numOfTables; i++) {
			result.add(createTable(Bytes.toBytes(i), "f:"));
		}
		return result;
	}

	public void fillRowLines(String tableName, String fileName)
			throws IOException {
		if (admin == null)
			admin = new HBaseAdmin(conf);
		if (!admin.tableExists(tableName)) {
			logger.error(tableName + " does not exist");
			return;
		}

	}

	static void toFileSystem(String inFile, String outFile) throws IOException {

	}

	/**
	 * the test
	 * 
	 * @throws IOException
	 */
	public void testHTable() throws IOException {
		byte[] value = "value".getBytes();

		try {
			new HTable(conf, nosuchTable);

		} catch (TableNotFoundException e) {
			// expected

		} catch (IOException e) {
			e.printStackTrace();
		}

		HTableDescriptor tableAdesc = new HTableDescriptor(tableAname);
		tableAdesc.addFamily(column);

		HTableDescriptor tableBdesc = new HTableDescriptor(tableBname);
		tableBdesc.addFamily(column);

		// create a couple of tables

		HBaseAdmin admin = new HBaseAdmin(conf);

		if (admin.tableExists(tableAname)) {
			admin.disableTable(tableAname);
			admin.deleteTable(tableAname);
		}
		if (admin.tableExists(tableBname)) {
			admin.disableTable(tableBname);
			admin.deleteTable(tableBname);
		}

		System.out.println("tabels:\n" + join(admin.listTables(), "\n"));

		admin.createTable(tableAdesc);
		admin.createTable(tableBdesc);

		// put some data into table A

		HTable a = new HTable(conf, tableAname);

		// Assert the metadata is good.
		HTableDescriptor meta = a.getConnection().getHTableDescriptor(
				tableAdesc.getName());
		logger.info(meta.toString());
		logger.info(tableAdesc.toString());

		BatchUpdate batchUpdate = new BatchUpdate(row);
		batchUpdate.put(familyName, value);
		a.commit(batchUpdate);

		// open a new connection to A and a connection to b

		HTable newA = new HTable(conf, tableAname);
		HTable b = new HTable(conf, tableBname);

		// copy data from A to B

		Scanner s = newA.getScanner(new byte[][] { familyName.getBytes() });

		try {
			for (RowResult r : s) {
				batchUpdate = new BatchUpdate(r.getRow());
				for (Map.Entry<byte[], Cell> e : r.entrySet()) {
					batchUpdate.put(e.getKey(), e.getValue().getValue());
				}
				b.commit(batchUpdate);
			}
		} finally {
			s.close();
		}

		// Opening a new connection to A will cause the tables to be reloaded

		try {
			HTable anotherA = new HTable(conf, tableAname);
			anotherA.get(row, familyName.getBytes());
		} catch (Exception e) {
			e.printStackTrace();
		}

		// We can still access A through newA because it has the table
		// information
		// cached. And if it needs to recalibrate, that will cause the
		// information
		// to be reloaded.

		// Test user metadata

		try {
			// make a modifiable descriptor
			HTableDescriptor desc = new HTableDescriptor(a.getTableDescriptor());
			// offline the table
			admin.disableTable(tableAname);
			// add a user attribute to HTD
			desc.setValue(attrName, attrValue);
			// add a user attribute to HCD
			for (HColumnDescriptor c : desc.getFamilies())
				c.setValue(attrName, attrValue);
			// update metadata for all regions of this table
			admin
					.modifyTable(tableAname, HConstants.MODIFY_TABLE_SET_HTD,
							desc);
			// enable the table
			admin.enableTable(tableAname);

			// test that attribute changes were applied
			desc = a.getTableDescriptor();
			if (Bytes.compareTo(desc.getName(), tableAname) != 0)
				logger.error("wrong table descriptor returned");
			// check HTD attribute
			value = desc.getValue(attrName);
			if (value == null)
				logger.error("missing HTD attribute value");
			if (Bytes.compareTo(value, attrValue) != 0)
				logger.error("HTD attribute value is incorrect");
			// check HCD attribute
			for (HColumnDescriptor c : desc.getFamilies()) {
				value = c.getValue(attrName);
				if (value == null)
					logger.error("missing HCD attribute value");
				if (Bytes.compareTo(value, attrValue) != 0)
					logger.error("HCD attribute value is incorrect");
			}

		} catch (Exception e) {
			e.printStackTrace();
		}
	}

	public static String join(Object[] ob, String dilema) {
		StringBuffer result = new StringBuffer();
		if (ob == null || ob.length == 0)
			return result.toString();
		result.append(ob[0]);
		for (int i = 1; i < ob.length; i++) {
			result.append(dilema + ob[i]);
		}
		return result.toString();
	}

	public static void main(String[] args) throws IOException {
		// new MCARInit().testHTable();
		MCARInit m = new MCARInit();
		m.deleteAllTables();
		// byte[] family=Bytes.toBytes("f:");
		String family = "f:";
		String tName = m.createTable(Bytes.toBytes("t"), family);
		HTable table = new HTable(m.conf, tName);

		Data data = new Data("data/in/00.arff");
		Set<Integer> lines = data.getLines();
		for (Integer line : lines) {
			BatchUpdate batchUpdate = new BatchUpdate(Bytes.toBytes(line));

			int[] row = data.getRow(line);
			for (int i = 0; i < row.length; i++) {
				batchUpdate.put(family + i, Bytes.toBytes(row[i]));
			}
			table.commit(batchUpdate);
		}

		System.out.println("dec " + table);

		Scanner s = table.getScanner(
				new byte[][] { Bytes.toBytes(family + 0) }, Bytes.toBytes(140),
				Bytes.toBytes(200));

		try {
			for (RowResult r : s) {
				System.out.println("line :" + Bytes.toInt(r.getRow()) + "  "
						+ Bytes.toInt(r.get("f:0").getValue()));
				for (Map.Entry<byte[], Cell> e : r.entrySet()) {
					System.out.println("\t--" + Bytes.toString(e.getKey())
							+ "-" + Bytes.toInt(e.getValue().getValue()));
				}
			}
		} finally {
			s.close();
		}
		// List<String> tables=m.createAllTables(3);

		System.out.println("tables " + join(m.admin.listTables(), "\n\t"));
	}
}
