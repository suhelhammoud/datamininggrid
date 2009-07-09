package init;

import org.apache.log4j.Logger;

import java.io.IOException;

import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.util.Pair;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.io.Text;
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
import org.apache.hadoop.mapred.TextInputFormat;
import org.apache.hadoop.mapred.TextOutputFormat;

import others.MyConstants;
import others.Tools;
import others.MyConstants.COUNTERS;
import dataTypes.IntListWritable;
import dataTypes.IntMap;
import dataTypes.fia.IntTextPair;
import dataTypes.fia.Two;

import java.util.Arrays;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

public class MapItemsToLowestLine extends MapReduceBase implements MyConstants,
Mapper<LongWritable, Text, IntTextPair, IntWritable>,
Reducer<IntTextPair, IntWritable, IntTextPair, IntWritable> {
	/**
	 * Logger for this class
	 */
	private static final Logger logger = Logger.getLogger(MapItemsToLowestLine.class);

	private static String SEP;

	@Override
	public void configure(JobConf job) {
		SEP = job.get(JOB.SEP.name(), ",");
	}

	public void map(LongWritable key, Text values,
			OutputCollector<IntTextPair, IntWritable> output, Reporter reporter)
	throws IOException {
		String[] line = values.toString().split(SEP);
		if (line.length < 2)
			return;
		IntWritable outValue = new IntWritable(Integer.valueOf(line[0]));

		IntTextPair outKey = new IntTextPair();
		for (int i = 1; i < line.length; i++) {
			outKey.set(i, line[i]);
			output.collect(outKey, outValue);
		}

	}

	@Override
	public void reduce(IntTextPair key, Iterator<IntWritable> values,
			OutputCollector<IntTextPair, IntWritable> output, Reporter reporter)
	throws IOException {
		IntWritable outValue = values.next();
		int min = outValue.get();

		while (values.hasNext()) {
			int i = values.next().get();
			if (i < min)
				min = i;
		}
		output.collect(key, new IntWritable(min));

	}

	public static void run(String inDir, String outDir) throws IOException {
		logger.info("("+inDir+","+outDir+")");

		JobConf job=new JobConf();
		job.setJobName(MapItemsToLowestLine.class.getName());


		job.setOutputKeyClass(IntTextPair.class);
		job.setOutputValueClass(IntWritable.class);

		job.setMapOutputKeyClass(IntTextPair.class);
		job.setMapOutputValueClass(IntWritable.class);

		job.setMapperClass(MapItemsToLowestLine.class);
		job.setCombinerClass(MapItemsToLowestLine.class);
		job.setReducerClass(MapItemsToLowestLine.class);

		job.setNumReduceTasks(1);

		job.setInputFormat(TextInputFormat.class);
		job.setOutputFormat(SequenceFileOutputFormat.class);

		Path inPath = new Path(inDir);
		Path outPath = new Path(outDir);

		FileSystem fs = FileSystem.get(job);
		fs.delete(outPath, true);
		FileInputFormat.setInputPaths(job, inPath);
		FileOutputFormat.setOutputPath(job, outPath);

		RunningJob rj = JobClient.runJob(job);
	}
	
}
