package old;

import org.apache.log4j.Logger;

import java.io.File;
import java.io.IOException;

import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.mapred.FileInputFormat;
import org.apache.hadoop.mapred.FileOutputFormat;
import org.apache.hadoop.mapred.JobClient;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.Mapper;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reporter;
import org.apache.hadoop.mapred.SequenceFileOutputFormat;
import org.apache.hadoop.mapred.TextInputFormat;
import org.apache.hadoop.mapred.TextOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import others.MyConstants;
import others.Tools;

import dataTypes.*;


public class MapperInitData extends MapReduceBase implements MyConstants,
		Mapper<LongWritable, Text, IntWritable, IntListWritable> {
	/**
	 * Logger for this class
	 */
	private static final Logger logger = Logger.getLogger(MapperInitData.class);

	public static String SEP;

	@Override
	public void configure(JobConf job) {
		SEP = job.get(JOB.SEP.name(), ",");
		
	}

	public void map(LongWritable key, Text values,
			OutputCollector<IntWritable, IntListWritable> output,
			Reporter reporter) throws IOException {
		//logger.info(values.toString());
		String[] line = values.toString().split(SEP);
		if (line.length < 2)
			return;
		IntWritable outKey = new IntWritable(Integer.valueOf(line[0]));
		int[] data = new int[line.length - 1];
		for (int i = 0; i < data.length; i++) {
			data[i] = Integer.valueOf(line[i + 1]);
		}
		IntListWritable outValue = new IntListWritable();
		outValue.setAll(data);
		output.collect(outKey, outValue);
	}

	/**
	 *  
	 * @param job
	 * @param inDir
	 * @param outDir
	 * leb
	 * @throws IOException
	 */
	public static void run(JobConf job, String inDir, String outDir)
			throws IOException {
		

		job.setJobName(MapperInitData.class.getName());

		job.setOutputKeyClass(IntWritable.class);
		job.setOutputValueClass(IntListWritable.class);

		job.setMapOutputKeyClass(IntWritable.class);
		job.setMapOutputValueClass(IntListWritable.class);

		job.setMapperClass(MapperInitData.class);

		job.setInputFormat(TextInputFormat.class);
		//job.setOutputFormat(TextOutputFormat.class);
		job.setOutputFormat(SequenceFileOutputFormat.class);

		Path inPath = new Path(inDir);
		Path outPath = new Path(outDir);

		FileSystem fs = FileSystem.get(job);
		fs.delete(outPath, true);
		FileInputFormat.setInputPaths(job, inPath);
		FileOutputFormat.setOutputPath(job, outPath);

		JobClient.runJob(job);
	}

	public static void main(String[] args) throws IOException {
		JobConf job=new JobConf();
		job.set("SEP", ",");
		run(job, "data/lined", "data/out/0");
		
	}
}
