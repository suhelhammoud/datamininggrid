package hdm;

import java.io.IOException;
import java.nio.ByteBuffer;

import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.apache.hadoop.hbase.client.HTable;

import others.MyConstants;


public class HUtil {

	public static final byte[] FAMILY = "f:".getBytes();
	// public static final byte[] columnQ=family;//(family+"data").getBytes();


	public static HTable create(HBaseConfiguration conf, byte[] columnName) {
		HTable result = null;
		HTableDescriptor d_tdata = new HTableDescriptor(columnName);
		HColumnDescriptor cdesc = new HColumnDescriptor("f:");
		cdesc.setMaxVersions(1);
		cdesc.setBlockCacheEnabled(true);
		d_tdata.addFamily(cdesc);

		try {
			HBaseAdmin admin = new HBaseAdmin(conf);

			if (admin.tableExists(d_tdata.getName())) {
				admin.disableTable(d_tdata.getName());
				admin.deleteTable(d_tdata.getName());

			}

			admin.createTable(d_tdata);
			HColumn.logger.info("create table " + d_tdata.getNameAsString());
			result = new HTable(conf, d_tdata.getName());
		} catch (IOException e) {
			e.printStackTrace();
		}
		return result;

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

	public static byte[] intArryToBytes(int[] a) {
		ByteBuffer bb = ByteBuffer.allocate((1 + a.length) * MyConstants.SIZEOF_INT);
		bb.putInt(a.length);
		for (int i : a)
			bb.putInt(i);
		return bb.array();
	}

	public static int[] byteArrayFromInts(byte[] a) {
		ByteBuffer bb = ByteBuffer.wrap(a);
		int[] result = new int[bb.getInt()];
		for (int i = 0; i < result.length; i++) {
			result[i] = bb.getInt();
		}
		return result;
	}

}
