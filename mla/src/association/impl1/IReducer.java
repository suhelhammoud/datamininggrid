package association.impl1;


import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reducer;
import org.apache.hadoop.mapred.Reporter;
import org.apache.log4j.Logger;

import utils.SetWritable;

import java.io.IOException;
import java.util.HashMap;
import java.util.Iterator;

public class IReducer extends MapReduceBase implements
Reducer<SetWritable, IntWritable, SetWritable, IntWritable> {

	static Logger log=Logger.getLogger(IReducer.class);

	private  static int SUPPORT;
	private static IntWritable freq=new IntWritable();


	public void reduce(SetWritable key, Iterator<IntWritable> values,
			OutputCollector<SetWritable, IntWritable> output, Reporter reporter) throws IOException {

		int sum=0;
		while (values.hasNext()) {
			sum+=values.next().get();

		}
		if (sum >= SUPPORT){
		    reporter.incrCounter(Count.count.Items_LEFT , 1);
			freq.set(sum);
			output.collect(key, freq);
		}
	}

	@Override
	public void configure(JobConf job) {
		super.configure(job);
		SUPPORT=job.getInt("SUPPORT", -1);
		if(SUPPORT == -1 ){
			log.error("can't get the support or iteration value");
		}

	}


}

class ICombiner extends MapReduceBase implements Reducer<SetWritable, IntWritable, SetWritable, IntWritable> {

	static Logger log=Logger.getLogger(ICombiner.class);
	private static IntWritable freq=new IntWritable();
	public void reduce(SetWritable key, Iterator<IntWritable> values,
			OutputCollector<SetWritable, IntWritable> output, Reporter reporter) throws IOException {
		int sum=0;
		while (values.hasNext()) {
			sum+=values.next().get();
		}
		freq.set(sum);
		output.collect(key, freq);
	}

}

