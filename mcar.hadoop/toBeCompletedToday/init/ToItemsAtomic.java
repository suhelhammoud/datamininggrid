package init;

import org.apache.log4j.Logger;

import java.io.IOException;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;

import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.VLongWritable;
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
import org.apache.hadoop.mapred.TextOutputFormat;
import org.apache.hadoop.mapred.lib.IdentityMapper;
import org.jcodings.Config;

import others.MyConstants;
import others.MyConstants.COUNTERS;
import others.MyConstants.JOB;
import others.MyConstants.TAG;

import dataTypes.IntListWritable;
import dataTypes.IntMap;
import dataTypes.fia.Two;

public class ToItemsAtomic extends MapReduceBase implements MyConstants,
Mapper<IntWritable,IntListWritable,Two,IntMap>,
Reducer<Two,IntMap,IntListWritable,IntMap> {
	/**
	 * Logger for this class
	 */
	private static final Logger logger = Logger.getLogger(ToItemsAtomic.class);

	int label=0;
	int support;
	float confidence;

	@Override
	public void configure(JobConf job) {
		label=job.getInt(JOB.label.name(), 0);
		support=job.getInt(JOB.support.name(),Integer.MAX_VALUE);
		confidence=job.getFloat(JOB.confidence.name(), Float.MAX_VALUE);

	}

	//{col,item} {label,line}
	public void map(IntWritable key, IntListWritable values,
			OutputCollector<Two,IntMap> output, Reporter reporter) throws IOException {
		IntMap outValue=new IntMap();
		outValue.add(values.get(label), key.get());

		for (int i = 0; i < label; i++) {

			Two outKey=new Two(i,values.get(i));	
			output.collect(outKey, outValue);
		}
		//emit the label
		{
			Two outKey=new Two(-1,-values.get(label));	
			output.collect(outKey, outValue);
		}
		//continue
		for (int i = label+1; i < values.size(); i++) {

			Two outKey=new Two(i,values.get(i));	
			output.collect(outKey, outValue);
		}


	}

	public void reduce(Two key, Iterator<IntMap> values,
			OutputCollector<IntListWritable,IntMap> output, Reporter reporter) throws IOException {

		IntMap outValue=new IntMap();
		while (values.hasNext()) {
			IntMap m=values.next();
			outValue.addMap(m);
		}
		
		//emit the label 
		if(key.p1==-1){
			IntListWritable outKey=new IntListWritable(1);
			outKey.add(key.p2);
			output.collect(outKey, outValue);
		}
		
		IntListWritable outKey=new IntListWritable(2);
		outKey.setAll(key.p1,key.p2);

		int[] tags=outValue.calc();

		if(tags[TAG.support.ordinal()]>=support){
			
			
			reporter.incrCounter(COUNTERS.items_left, 1);

			//TODO do test to to be deleted later
			if(tags[TAG.minline.ordinal()] != key.p2){
				logger.error("not equals "+ key+ "\t"+ Arrays.toString(tags));
				return;
			}

			if(tags[TAG.confidence.ordinal()] >= confidence){
				reporter.incrCounter(COUNTERS.candidate_rules, 1);
				outValue.setIsRule(true);
			}
			output.collect(outKey, outValue);
		}
	}

	/**
	 * 
	 * @param job
	 * inDir =  input_dir: (data/in/input)
	 * outDir=  items_dir+"/1":( data/items/1)
	 * label,support,confidence
	 * @param inDir TODO
	 * @param outDir TODO
	 * @return {COUNTERS.items_left, COUNTERS.candidate_rules}
	 * @throws IOException
	 */
	public static long[] run(String inDir, String outDir,int label,int support,float confidence)throws IOException{
		logger.info("("+inDir+", "+outDir+")");
		JobConf job=new JobConf();
		job.setJobName(ToItemsAtomic.class.getName());
		job.setInt(JOB.support.name(), support);
		job.set(JOB.confidence.name(),String.valueOf(confidence));
		job.setInt(JOB.label.name(), label);

		job.setOutputKeyClass(IntListWritable.class);
		job.setOutputValueClass(IntMap.class);

		job.setMapOutputKeyClass(Two.class);
		job.setMapOutputValueClass(IntMap.class);

		job.setMapperClass(ToItemsAtomic.class);
		job.setCombinerClass(AtomicCombiner.class);
		job.setReducerClass(ToItemsAtomic.class);

		job.setNumMapTasks(1);
		job.setNumReduceTasks(1);

		job.setInputFormat(SequenceFileInputFormat.class);
		job.setOutputFormat(SequenceFileOutputFormat.class);
		//		job.setInputFormat(TextInputFormat.class);
		//		job.setOutputFormat(TextOutputFormat.class);


		Path inPath = new Path(inDir);
		Path outPath = new Path(outDir);

		FileSystem fs = FileSystem.get(job);
		fs.delete(outPath, true);
		FileInputFormat.setInputPaths(job, inPath);
		FileOutputFormat.setOutputPath(job, outPath);

		RunningJob rj=JobClient.runJob(job);
		Counters counters=rj.getCounters();
	
		System.out.println(counters);
		return new long[]{counters.getCounter(COUNTERS.items_left),
				counters.getCounter(COUNTERS.candidate_rules)};
	}

	public static void main(String[] args) throws IOException {
		JobConf job=new JobConf();


		long[] conter=run("data/in/input", "data/items/1",3,0,0f);
		System.out.println("result "+ Arrays.toString(conter));
	}




}

class AtomicCombiner extends MapReduceBase implements Reducer<Two,IntMap,Two,IntMap> {
	/**
	 * Logger for this class
	 */
	private static final Logger logger = Logger.getLogger(AtomicCombiner.class);

	public void reduce(Two key, Iterator<IntMap> values,
			OutputCollector<Two,IntMap> output, Reporter reporter) throws IOException {

		IntMap outValue=new IntMap();
		while (values.hasNext()){
			IntMap v=values.next();
			outValue.addMap(v);
		}
		output.collect(key, outValue);
	}

}
