package init;

import org.apache.log4j.Logger;

import java.io.IOException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.FileInputFormat;
import org.apache.hadoop.mapred.FileOutputFormat;
import org.apache.hadoop.mapred.JobClient;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.Mapper;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reporter;
import org.apache.hadoop.mapred.RunningJob;
import org.apache.hadoop.mapred.SequenceFileOutputFormat;
import org.apache.hadoop.mapred.TextInputFormat;

import others.MyConstants;
import others.Tools;
import others.MyConstants.JOB;
import dataTypes.IntListWritable;
import dataTypes.fia.IntTextPair;

public class ReplaceItemsWithLines extends MapReduceBase implements MyConstants,
Mapper<LongWritable,Text,IntWritable,IntListWritable> {
	/**
	 * Logger for this class
	 */
	private static final Logger logger = Logger.getLogger(ReplaceItemsWithLines.class);

	private static String SEP = ",";
	Map<IntTextPair, Integer> map=new HashMap<IntTextPair, Integer>();

	@Override
	
	public void configure(JobConf job) {
		SEP=job.get(JOB.SEP.name(), ",");
		String mapDir=job.get(JOB.map_dir.name(), "data/in/map");
		readMap(job,mapDir);
	}
	public void readMap(JobConf job,String mapDir) {
		try {
			FileSystem fs = FileSystem.get(job);
			Path srcPath=new Path(mapDir);
			if (! fs.exists(srcPath)) {
				System.out.println("No map file found");
				return ;
			}
			List<Path> paths=Tools.listAllFiles(srcPath);
			for (Path path : paths) {
				SequenceFile.Reader reader = new SequenceFile.Reader(fs, path, job);

				IntTextPair key = new IntTextPair();
				IntWritable value = new IntWritable();
				while (reader.next(key, value)) {
					map.put(key.copy(), value.get());
				}
				reader.close();
			}			
			
		} catch (IOException e) {
			e.printStackTrace();
		}
	}
	@Override
	public void map(LongWritable key, Text value,
			OutputCollector<IntWritable, IntListWritable> output,
			Reporter reporter) throws IOException {
		String[] line = value.toString().split(SEP);
		if (line.length < 2)
			return;
		IntWritable outKey = new IntWritable(Integer.valueOf(line[0]));
		IntListWritable outValue=new IntListWritable(line.length-1);
		
		IntTextPair item=new IntTextPair();
		for (int i = 1; i < line.length; i++) {
			item.set(i,line[i]);
			Integer v=map.get(item);
			if(v == null)v=-1;
			outValue.add(v);
		}
		if(outValue != null)
			output.collect(outKey,outValue);
	}
	
	public static void run(String inDir, String outDir, String mapDir) throws IOException{
		logger.info("("+inDir+","+outDir+","+mapDir+")");

		JobConf job=new JobConf();
		job.setJobName(ReplaceItemsWithLines.class.getName());
		job.set(JOB.map_dir.name(), "data/in/map");
		
		job.setOutputKeyClass(IntWritable.class);
		job.setOutputValueClass(IntListWritable.class);

		job.setMapOutputKeyClass(IntWritable.class);
		job.setMapOutputValueClass(IntListWritable.class);

		job.setMapperClass(ReplaceItemsWithLines.class);

		job.setNumReduceTasks(1);

		//job.setInputFormat(SequenceFileInputFormat.class);
		job.setInputFormat(TextInputFormat.class);		
		job.setOutputFormat(SequenceFileOutputFormat.class);
		//job.setOutputFormat(TextOutputFormat.class);

		Path inPath = new Path(inDir);
		Path outPath = new Path(outDir);

		FileSystem fs = FileSystem.get(job);
		fs.delete(outPath, true);
		FileInputFormat.setInputPaths(job, inPath);
		FileOutputFormat.setOutputPath(job, outPath);

		RunningJob rj=JobClient.runJob(job);
	}

}