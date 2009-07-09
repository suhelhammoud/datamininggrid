package init;

import org.apache.log4j.Logger;

import java.io.IOException;
import java.util.Collection;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.Set;
import java.util.TreeSet;

import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.mapred.Counters;
import org.apache.hadoop.mapred.FileInputFormat;
import org.apache.hadoop.mapred.FileOutputFormat;
import org.apache.hadoop.mapred.JobClient;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.Mapper;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reducer;
import org.apache.hadoop.mapred.Reporter;
import org.apache.hadoop.mapred.RunningJob;
import org.apache.hadoop.mapred.SequenceFileInputFormat;
import org.apache.hadoop.mapred.SequenceFileOutputFormat;

import dataTypes.IntListWritable;
import dataTypes.IntMap;

import others.MyConstants;
import others.MyConstants.TAG;

public class RulesToLines extends MapReduceBase implements MyConstants,
	Mapper<IntListWritable,IntMap,IntWritable,IntListWritable>,
	Reducer<IntWritable, IntListWritable, IntWritable, IntListWritable> {
	/**
	 * Logger for this class
	 */
	private static final Logger logger = Logger.getLogger(RulesToLines.class);

	@Override
	public void map(IntListWritable key, IntMap value,
			OutputCollector<IntWritable, IntListWritable> output,
			Reporter reporter) throws IOException {
		int length=key.size();
		
		//if label emit and exit
		if(length==1){
			for (List<Integer> list : value.values()) {
				for (Integer i : list) {
					output.collect(new IntWritable(i), key);
				}
			}
			return;
		}
		//if not rule exit
		//if (! value.isRule() )return;
		
		
		// add support and confidence to the list
		key.add(0,value.getSupport());
		key.add(0,value.getConfidene());
//		
		
		for (List<Integer> list : value.values()) {
			for (Integer i : list) {
				output.collect(new IntWritable(i), key);
			}
		}


		
	}

	@Override
	public void reduce(IntWritable key, Iterator<IntListWritable> values,
			OutputCollector<IntWritable, IntListWritable> output,
			Reporter reporter) throws IOException {
		IntListWritable labelWritable=null;
		IntListWritable outValue=new IntListWritable(2);
		outValue.setAll(0,0);
		
	
		while(values.hasNext()){
			IntListWritable vRule=values.next();
			if(vRule.size()==1){
				labelWritable=new IntListWritable(vRule);
				continue;
			}
			int dif=RulesToLines.compareTwoRules(vRule, outValue);
			logger.error("===="+dif+"======= "+ key+"\t"+outValue+","+vRule);

			if(dif>0)outValue=new IntListWritable(vRule);
		}
		
		//logger.error("outvalue "+ outValue);

		if(outValue.size()<2)return;//label only
		
		if(labelWritable !=null){
			outValue.add(labelWritable.get(0));
		}else{
			logger.error("no label "+key+"\t"+outValue+ "\t"+labelWritable);
		}
		output.collect(key, outValue);		
	}
	static int compareTwoRules(List<Integer> list1,List<Integer> list2){
		int confidence1=list1.get(0);
		int confidence2=list2.get(0);
		int support1=list1.get(1);
		int support2=list2.get(1);
		
		int dif=confidence1-confidence2;
		if(dif != 0)return dif;

		//support
		dif= support1-support2;
		if(dif != 0)return dif;
		
		//column length
		dif=list1.size()-list2.size();
		if(dif !=0) return -dif;//minus
		
		
		for (int i = 2; i < list1.size() && i<list2.size(); i++) {
			dif=list1.get(i)-list2.get(i);
			if(dif!=0)return dif;
		}

		//this is error case
		//logger.error("equal case "+ list1+", "+ list2);
		return 0;
		
	}
	
	public static void run(String inDir,String outDir,String labelDir) throws IOException{
		JobConf job=new JobConf();
		job.setJobName(RulesToLines.class.getName());

	
		job.setOutputKeyClass(IntWritable.class);
		job.setOutputValueClass(IntListWritable.class);

		job.setMapOutputKeyClass(IntWritable.class);
		job.setMapOutputValueClass(IntListWritable.class);

		job.setMapperClass(RulesToLines.class);
		job.setCombinerClass(RulesToLinesCombiner.class);
		job.setReducerClass(RulesToLines.class);
		//job.setReducerClass(RulesToLinesTestReducer.class);

		job.setNumMapTasks(1);
		job.setNumReduceTasks(1);

		job.setInputFormat(SequenceFileInputFormat.class);
		job.setOutputFormat(SequenceFileOutputFormat.class);
		// job.setInputFormat(TextInputFormat.class);
		// job.setOutputFormat(TextOutputFormat.class);

		Path inPath = new Path(inDir);
		Path outPath = new Path(outDir);
		//Path labelsPath=new Path(labelDir);
		FileSystem fs = FileSystem.get(job);
		fs.delete(outPath, true);
		
		FileInputFormat.setInputPaths(job, inPath);
		//FileInputFormat.addInputPath(job, labelsPath);
		
		FileOutputFormat.setOutputPath(job, outPath);

		RunningJob rj = JobClient.runJob(job);
		Counters counters = rj.getCounters();
	}
	
	public static void main(String[] args) {
		try {
			RulesToLines.run("data/items/*", "data/lines/rules", "data/labels");
		} catch (IOException e) {
			e.printStackTrace();
		}
	}
}

class RulesToLinesCombiner  extends MapReduceBase implements MyConstants,
Reducer<IntWritable, IntListWritable, IntWritable, IntListWritable> {
	
	private static final Logger logger = Logger.getLogger(RulesToLinesCombiner.class);

	@Override
	public void reduce(IntWritable key, Iterator<IntListWritable> values,
			OutputCollector<IntWritable, IntListWritable> output,
			Reporter reporter) throws IOException {
		IntListWritable labelWritable=null;
		IntListWritable outValue=new IntListWritable(2);
		outValue.setAll(0,0);
		

		while(values.hasNext()){
			IntListWritable vRule=values.next();
			if(vRule.size()==1){
				labelWritable=new IntListWritable(vRule);
				continue;
			}
			int dif=RulesToLines.compareTwoRules(vRule, outValue);

			if(dif>0)outValue=new IntListWritable(vRule);
		}
		
		if(labelWritable != null)
			output.collect(key, labelWritable);
		else
			logger.error("no label "+key+"\t"+outValue+ "\t"+labelWritable);
		output.collect(key, outValue);		
	}
	

}


class RulesToLinesTestReducer2 extends MapReduceBase implements MyConstants,
Reducer<IntWritable, IntListWritable, IntWritable, IntListWritable> {

	@Override
	public void reduce(IntWritable key, Iterator<IntListWritable> values,
			OutputCollector<IntWritable, IntListWritable> output,
			Reporter reporter) throws IOException {
			
		while(values.hasNext())
			output.collect(key, values.next());
	}
	
	
}