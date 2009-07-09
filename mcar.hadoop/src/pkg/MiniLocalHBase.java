package pkg;

import java.io.IOException;

//import org.apache.hadoop.dfs.MiniDFSCluster;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.MiniHBaseCluster;
import org.apache.hadoop.hbase.util.FSUtils;
import org.apache.hadoop.hdfs.MiniDFSCluster;

//import org.apache.hadoop.hdfs.MiniDFSCluster;  

/**
 * 20. * Starts a small local DFS and HBase cluster. 21. * 22. * @author Lars
 * George 23.
 */
public class MiniLocalHBase implements HConstants {

	// MiniMRCluster
	static HBaseConfiguration conf = null;
	// static MiniDFSCluster dfs = null;
	static MiniHBaseCluster hbase = null;

	/**
	 * 31. * Main entry point to this class. 32. * 33. * @param args The command
	 * line arguments. 34.
	 */
	public static void main(String[] args) {
		try {

			int n = args.length > 0 && args[0] != null ? Integer
					.parseInt(args[0]) : 2;

			conf = new HBaseConfiguration();
			Path parentdir = new Path("tmp/data");

			conf.set(HConstants.HBASE_DIR, parentdir.toString());
			conf.set("fs.default.name", "file:///");
			// conf.set("hbase.rootdir", "file:///");

			// dfs = new MiniDFSCluster(conf, 1, true, (String[]) null);
			final FileSystem fs = FileSystem.getLocal(conf);
			// fs.mkdirs(new Path("testtesttesttest"));
			// final FileSystem fs = dfs.getFileSystem();

			// set file system to the mini dfs just started up

			// conf.set("fs.default.name",fs.getUri().toString());

			// FileSystem fs = dfs.getFileSystem();
			if (fs.exists(parentdir)) {
				fs.delete(parentdir, true);
			}

			parentdir = fs.makeQualified(new Path(conf
					.get(HConstants.HBASE_DIR)));

			FSUtils.setVersion(fs, parentdir);
			conf.set(HConstants.REGIONSERVER_ADDRESS, HConstants.DEFAULT_HOST
					+ ":0");
			// disable UI or it clashes for more than one RegionServer
			conf.set("hbase.regionserver.info.port", "-1");
			hbase = new MiniHBaseCluster(conf, n);
			// add close hook
			Runtime.getRuntime().addShutdownHook(new Thread() {
				public void run() {
					hbase.shutdown();

					try {
						if (fs != null)
							fs.close();
					} catch (IOException e) {
						System.err.println("error closing file system: " + e);
					}

				}
			});
		} catch (Exception e) {
			e.printStackTrace();
		}
	} // main

} // MiniLocalHBase 