package init;

import org.apache.log4j.Logger;

import java.io.IOException;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;

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

public class RulesToItems extends MapReduceBase implements
		Mapper<IntWritable, IntListWritable, IntListWritable, IntListWritable>, 
		Reducer<IntListWritable, IntListWritable, IntListWritable, IntListWritable> {
	/**
	 * Logger for this class
	 */
	private static final Logger logger = Logger.getLogger(RulesToItems.class);

	@Override
	public void map(IntWritable key, IntListWritable value,
			OutputCollector<IntListWritable, IntListWritable> output,
			Reporter reporter) throws IOException {
		IntListWritable labelWritable=new IntListWritable(1);
		labelWritable.add2(value.get(value.size()-1),1);
		value.remove(value.size()-1);
		//value.remove(0);value.remove(0);
		output.collect(value, labelWritable);
	}

	@Override
	public void reduce(IntListWritable key, Iterator<IntListWritable> values,
			OutputCollector<IntListWritable, IntListWritable> output,
			Reporter reporter) throws IOException {

		Map<Integer, Integer> tmap=new HashMap<Integer, Integer>();
		while (values.hasNext()) {
			IntListWritable item = values.next();
			for (int i = 0; i < item.size(); i+=2) {
				Integer lbl=item.get(i);
				int 	occ=item.get(i+1);
				Integer count=tmap.get(lbl);
				if(count==null){
					tmap.put(lbl, occ);
				}else{
					tmap.put(lbl, occ+count);
				}				
			}
		}

		IntListWritable outvalue=new IntListWritable(tmap.size()*2);
		for (Map.Entry<Integer, Integer> iter : tmap.entrySet()) {
			outvalue.add(iter.getKey());
			outvalue.add(iter.getValue());
		}
		output.collect(key, outvalue);
	}
	public static void run(String inDir,String outDir) throws IOException{
		JobConf job=new JobConf();
		job.setJobName(RulesToItems.class.getName());

	
		job.setOutputKeyClass(IntListWritable.class);
		job.setOutputValueClass(IntListWritable.class);

		job.setMapOutputKeyClass(IntListWritable.class);
		job.setMapOutputValueClass(IntListWritable.class);

		job.setMapperClass(RulesToItems.class);
		job.setCombinerClass(RulesToItems.class);
		job.setReducerClass(RulesToItems.class);
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
			RulesToLines.run("data/items/*", "data/rules/lines", "");
			RulesToItems.run("data/rules/lines", "data/rules/items");
		} catch (IOException e) {
			e.printStackTrace();
		}
	}

}
