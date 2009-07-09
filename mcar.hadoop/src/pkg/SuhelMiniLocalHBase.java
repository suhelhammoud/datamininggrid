package pkg;

import java.io.IOException;

import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.MiniHBaseCluster;
import org.apache.hadoop.hbase.util.FSUtils;

//import org.apache.hadoop.hdfs.MiniDFSCluster;  

/**
 * 20. * Starts a small local DFS and HBase cluster. 21. * 22. * @author Lars
 * George 23.
 */
public class SuhelMiniLocalHBase {

	// MiniMRCluster
	static HBaseConfiguration conf = null;
	static MiniHBaseCluster hbase = null;

	/**
	 * 31. * Main entry point to this class. 32. * 33. * @param args The command
	 * line arguments. 34.
	 */
	public static void main(String[] args) {
		try {
			int n = args.length > 0 && args[0] != null ? Integer
					.parseInt(args[0]) : 4;
			conf = new HBaseConfiguration();
			// dfs = new MiniDFSCluster(conf, 1, true, (String[]) null);

			// set file system to the mini dfs just started up

			// FileSystem fs = dfs.getFileSystem();
			FileSystem fs = FileSystem.getLocal(conf);
			conf.set("fs.default.name", fs.getUri().toString());
			// Path parentdir = fs.getHomeDirectory();
			Path parentdir = new Path(fs.getHomeDirectory().toString()
					+ "/hbase.dir");
			System.out.println("homeDir " + parentdir);
			conf.set(HConstants.HBASE_DIR, parentdir.toString());
			fs.mkdirs(parentdir);
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
				}
			});
		} catch (Exception e) {
			e.printStackTrace();
		}
	} // main

} // MiniLocalHBase 