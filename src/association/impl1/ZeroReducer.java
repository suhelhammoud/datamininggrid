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
import java.util.Iterator;

public class ZeroReducer extends MapReduceBase implements
Reducer<IntWritable, IntWritable, IntWritable, IntWritable> {

	static Logger log=Logger.getLogger(ZeroReducer.class);

	private  static int SUPPORT;


	public void reduce(IntWritable key, Iterator<IntWritable> values,
			OutputCollector<IntWritable,IntWritable> output, Reporter reporter) throws IOException {
		int sum=0;
		while (values.hasNext()) {    	
			sum+=values.next().get();
		}
		if(sum >= SUPPORT){
		    reporter.incrCounter(Count.count.Items_LEFT , 1);
		    //SetWritable sw=new SetWritable();
		    //sw.add(key);
			output.collect(key, new IntWritable(sum));
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

class ZeroCombiner extends MapReduceBase implements
Reducer<IntWritable, IntWritable, IntWritable, IntWritable> {

	static Logger log=Logger.getLogger(ZeroCombiner.class);

	public void reduce(IntWritable key, Iterator<IntWritable> values,
			OutputCollector<IntWritable, IntWritable> output, Reporter reporter) throws IOException {
		int sum=0;
		while (values.hasNext()) {    	
			sum+=values.next().get();
		}
		output.collect(key, new IntWritable(sum));
	}
}
