package pkg;

import java.io.ByteArrayOutputStream;
import java.io.DataInput;
import java.io.IOException;
import java.io.ObjectOutput;
import java.io.ObjectOutputStream;

import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.LocalHBaseCluster;
import org.apache.hadoop.hbase.util.Bytes;

public class L {

	public static void main(String[] args) throws IOException {
		LocalHBaseCluster lhc = new LocalHBaseCluster(new HBaseConfiguration());
		lhc.startup();

	}
}
