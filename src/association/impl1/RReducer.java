package association.impl1;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reducer;
import org.apache.hadoop.mapred.Reporter;
import org.apache.log4j.Logger;

import rules.Rule;
import utils.SetWritable;

public class RReducer extends MapReduceBase implements Reducer<SetWritable, ItemFreq, Rl, IntWritable> {
	static Logger log=Logger.getLogger(RReducer.class);
	static int total;
	static IntWritable zero=new IntWritable(1);
	public void reduce(SetWritable key, Iterator<ItemFreq> values,
			OutputCollector<Rl,IntWritable> output, Reporter reporter) throws IOException {

		ItemFreq basicItemFreq=null;
		int basicFreq=0;

		List<ItemFreq> lstItmFreq=new ArrayList<ItemFreq>();
		while(values.hasNext()){
			ItemFreq v=values.next();
			if(v.size()==0){
				basicItemFreq=v;
				basicFreq=v.freq.get();
				break;
			}
			lstItmFreq.add(v);
		}
		if(basicItemFreq ==null){
			log.error("not basic item sets found");
			return;
		}
			
		for (ItemFreq v : lstItmFreq) {
			Rl rule=new Rl(key,basicFreq, v.item,v.freq.get());

			if (rule.isPassed()){
				output.collect(rule, zero);
				//System.out.println(rule.toString());
			}

		}

		while (values.hasNext()) {
			ItemFreq v=values.next();
			Rl rule=new Rl(key,basicFreq, v.item,v.freq.get());
			if (rule.isPassed()){
				output.collect(rule, zero);
				//System.out.println(rule.toString());
			}
		}
	}


	@Override
	public void configure(JobConf job) {
		// TODO Auto-generated method stub
		super.configure(job);
		Rule.setTotal(job.getInt("TOTAL", 9));
		Rule.setCONFIDENCE(job.getFloat("CONFIDENCE", 0));
		
	}

}
