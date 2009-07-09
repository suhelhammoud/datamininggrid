package pkg;

import org.apache.log4j.Logger;

import java.io.IOException;
import java.util.Map;

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

public class MyHtable implements HConstants {
	/**
	 * Logger for this class
	 */
	private static final Logger logger = Logger.getLogger(MyHtable.class);

	private static final HColumnDescriptor column = new HColumnDescriptor(
			COLUMN_FAMILY);

	private static final byte[] nosuchTable = Bytes.toBytes("nosuchTable");
	private static final byte[] tableAname = Bytes.toBytes("tableA");
	private static final byte[] tableBname = Bytes.toBytes("tableB");

	private static final byte[] row = Bytes.toBytes("row");

	private static final byte[] attrName = Bytes.toBytes("TESTATTR");
	private static final byte[] attrValue = Bytes.toBytes("somevalue");

	HBaseConfiguration conf = new HBaseConfiguration();

	/**
	 * the test
	 * 
	 * @throws IOException
	 */
	public void testHTable() throws IOException {
		byte[] value = "value".getBytes(UTF8_ENCODING);

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
		batchUpdate.put(COLUMN_FAMILY, value);
		a.commit(batchUpdate);

		// open a new connection to A and a connection to b

		HTable newA = new HTable(conf, tableAname);
		HTable b = new HTable(conf, tableBname);

		// copy data from A to B

		Scanner s = newA.getScanner(COLUMN_FAMILY_ARRAY, EMPTY_START_ROW);

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
			anotherA.get(row, COLUMN_FAMILY);
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
		result.append(ob[0].toString());
		for (int i = 1; i < ob.length; i++) {
			result.append(dilema + ob[i].toString());
		}
		return result.toString();
	}

	public static void main(String[] args) throws IOException {
		new MyHtable().testHTable();
	}
}
