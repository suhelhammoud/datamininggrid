package mcar.hadoop;

import java.io.IOException;
import java.util.Iterator;

import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reducer;
import org.apache.hadoop.mapred.Reporter;

public class ReducerGetAtomics extends MapReduceBase implements Reducer<IntWritable,IntWritable,IntWritable,IntWritable> {

	private int support;
	private String atomicTableName;
	private HTable table;
	
	@Override
	public void configure(JobConf job) {
		support=job.getInt("support", 1);
		atomicTableName=job.get("atomicTableName", "1");
		try {
			table=new HTable(atomicTableName);
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		super.configure(job);
	}
	public void reduce(IntWritable _key, Iterator<IntWritable> values,
			OutputCollector<IntWritable,IntWritable> output, Reporter reporter) throws IOException {
		// replace KeyType with the real type of your key
		
		
	}

}
