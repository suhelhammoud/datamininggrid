package init;

import java.io.IOException;
import java.util.Iterator;
import java.util.List;

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
import org.apache.hadoop.mapred.TextOutputFormat;

import others.MyConstants;
import others.MyConstants.COUNTERS;
import others.MyConstants.JOB;

import dataTypes.IntListWritable;
import dataTypes.IntMap;
import dataTypes.fia.IntTextPair;
import dataTypes.fia.Two;

public class ToLines extends MapReduceBase implements MyConstants,
		Mapper<IntListWritable, IntMap, IntWritable, IntListWritable>,
		Reducer<IntWritable, IntListWritable, IntWritable, IntListWritable> {

	@Override
	public void map(IntListWritable key, IntMap value,
			OutputCollector<IntWritable, IntListWritable> output,
			Reporter reporter) throws IOException {
		IntListWritable outValue = key;
		for (List<Integer> list : value.values()) {
			for (Integer i : list) {
				output.collect(new IntWritable(i), outValue);
			}
		}
	}

	@Override
	public void reduce(IntWritable key, Iterator<IntListWritable> values,
			OutputCollector<IntWritable, IntListWritable> output,
			Reporter reporter) throws IOException {

		IntListWritable outValue = new IntListWritable();
		while (values.hasNext()) {
			IntListWritable v = values.next();
			outValue.addAll(v);
		}
		if(outValue.size()==1)return;
		output.collect(key, outValue);

	}

	public static void run(String itemsDir,String linesDir,String labelsDir, int iteration) throws IOException {
		
		JobConf job=new JobConf();
		job.setJobName(ToLines.class.getName());

	
		job.setOutputKeyClass(IntWritable.class);
		job.setOutputValueClass(IntListWritable.class);

		job.setMapOutputKeyClass(IntWritable.class);
		job.setMapOutputValueClass(IntListWritable.class);

		job.setMapperClass(ToLines.class);
		job.setCombinerClass(ToLines.class);
		job.setReducerClass(ToLines.class);

		job.setNumMapTasks(1);
		job.setNumReduceTasks(1);

		job.setInputFormat(SequenceFileInputFormat.class);
		job.setOutputFormat(SequenceFileOutputFormat.class);
		// job.setInputFormat(TextInputFormat.class);
		// job.setOutputFormat(TextOutputFormat.class);

		Path inPath = new Path(itemsDir+"/"+iteration);
		Path outPath = new Path(linesDir+"/"+iteration);
		Path labelsPath=new Path(labelsDir);
		FileSystem fs = FileSystem.get(job);
		fs.delete(outPath, true);
		
		FileInputFormat.setInputPaths(job, inPath);
		//FileInputFormat.addInputPath(job, labelsPath);
		
		FileOutputFormat.setOutputPath(job, outPath);

		RunningJob rj = JobClient.runJob(job);
		Counters counters = rj.getCounters();

	}

	

}
