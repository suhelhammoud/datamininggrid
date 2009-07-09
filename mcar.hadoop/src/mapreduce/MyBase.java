package mapreduce;

import java.io.IOException;
import java.util.Iterator;

import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.Mapper;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reducer;
import org.apache.hadoop.mapred.Reporter;

import others.MyConstants;

public abstract class MyBase<K1, V1, K2, V2, K3, V3>  {

	abstract public void map(K1 key, V1 value, OutputCollector<K2, V2> output,
			Reporter reporter) throws IOException ;

	abstract public void reduce(K2 key, Iterator<V2> values,
			OutputCollector<K3, V3> output, Reporter reporter)
			throws IOException;

	public void configure(JobConf job) {
		// TODO Auto-generated method stub
	}
	
	

}
