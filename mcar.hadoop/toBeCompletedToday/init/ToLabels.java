package init;

import org.apache.log4j.Logger;

import java.io.IOException;
import java.util.Iterator;

import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
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
import others.MyConstants.JOB;

public class ToLabels extends MapReduceBase implements MyConstants,
	Mapper<IntWritable,IntListWritable,IntWritable,IntListWritable>,
	Reducer<IntWritable,IntListWritable,IntListWritable,IntMap> {
	/**
	 * Logger for this class
	 */
	private static final Logger logger = Logger.getLogger(ToLabels.class);

	int label;
	
	@Override
	public void configure(JobConf job) {
		label=job.getInt(JOB.label.name(), -1);
	}
	@Override
	public void map(IntWritable key, IntListWritable value,
			OutputCollector<IntWritable, IntListWritable> output, Reporter reporter)
			throws IOException {

		IntListWritable outValue=new IntListWritable(1);
		outValue.setAll(key.get());
		output.collect(new IntWritable(value.get(label)), outValue);
		
	}
	@Override
	public void reduce(IntWritable key, Iterator<IntListWritable> values,
			OutputCollector<IntListWritable, IntMap> output,
			Reporter reporter) throws IOException {
		IntListWritable outList=new IntListWritable();
		while(values.hasNext())
			outList.addAll(values.next());
		
		IntMap outValue=new IntMap();
		outValue.addList(key.get(), outList);
		
		IntListWritable outKey=new IntListWritable(1);
		outKey.add(-key.get());
		output.collect(outKey, outValue);
	}

	public static void run(String inDir,String outDir,int label) throws IOException{
		JobConf job=new JobConf();
		job.setJobName(ToLabels.class.getName());
		job.setInt(JOB.label.name(), label);

		
		job.setOutputKeyClass(IntListWritable.class);
		job.setOutputValueClass(IntMap.class);

		job.setMapOutputKeyClass(IntWritable.class);
		job.setMapOutputValueClass(IntListWritable.class);

		job.setMapperClass(ToLabels.class);
		 job.setCombinerClass(ToLabelsCombiner.class);
		 job.setReducerClass(ToLabels.class);

		job.setNumMapTasks(1);
		job.setNumReduceTasks(1);

		job.setInputFormat(SequenceFileInputFormat.class);
		job.setOutputFormat(SequenceFileOutputFormat.class);
		// job.setInputFormat(TextInputFormat.class);
		// job.setOutputFormat(TextOutputFormat.class);

		Path inPath = new Path(inDir);
		Path outPath = new Path(outDir);

		FileSystem fs = FileSystem.get(job);
		fs.delete(outPath, true);
		FileInputFormat.setInputPaths(job, inPath);
		FileOutputFormat.setOutputPath(job, outPath);

		RunningJob rj = JobClient.runJob(job);
		Counters counters = rj.getCounters();
		System.out.println(counters);
		
	}
	
}

class ToLabelsCombiner extends MapReduceBase implements
Reducer<IntWritable,IntListWritable,IntWritable,IntListWritable> {
	/**
	 * Logger for this class
	 */
	private static final Logger logger = Logger.getLogger(ToLabelsCombiner.class);

	@Override
	public void reduce(IntWritable key, Iterator<IntListWritable> values,
			OutputCollector<IntWritable, IntListWritable> output,
			Reporter reporter) throws IOException {
		IntListWritable outValue=new IntListWritable();
		while(values.hasNext())
			outValue.addAll(values.next());
		
		output.collect(key, outValue);
			
	}
	
}