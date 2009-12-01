package association.impl1;


import java.io.IOException;

import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.mapred.Counters;
import org.apache.hadoop.mapred.JobClient;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.Mapper;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reporter;
import org.apache.hadoop.mapred.RunningJob;
import org.apache.hadoop.mapred.SequenceFileInputFormat;
import org.apache.hadoop.mapred.SequenceFileOutputFormat;

import utils.SetWritable;

public class RemoveNotFrequentMapper extends MapReduceBase implements Mapper<IntWritable,SetWritable,IntWritable,SetWritable> {

	ItemMap map;
	
	public void map(IntWritable key, SetWritable values,
			OutputCollector<IntWritable,SetWritable> output, Reporter reporter) throws IOException {
		SetWritable sw=new SetWritable();

		SetWritable one=new SetWritable();
		for (Integer atomic : values) {
			one.clear();
			one.add(atomic);
			if( map.containsKey(one))sw.add(atomic);
		}
		if(sw.size() > 0)
			output.collect(key, sw);
	}

	@Override
	public void configure(JobConf job) {
		// TODO Auto-generated method stub
		super.configure(job);
		map=new ItemMap();
		String freqDir=job.get("freqDir");
		map.load(freqDir+"/1",job);
		//System.out.println("map fre1 ="+map.toString());
	}
	
	public static void runJob(String input,String output,String freqDir, Param param){
		try {
			JobConf conf=new JobConf(RemoveNotFrequentMapper.class);
			conf.setJobName("RemoveNotFrequentMapper");
			
			conf.set("freqDir", freqDir);
			Path outPath = new Path(output);
			FileSystem fs = FileSystem.get(conf);
			if (fs.exists(outPath)) {
				fs.delete(outPath);
			}

			conf.setMapOutputKeyClass(IntWritable.class);
			conf.setMapOutputValueClass(SetWritable.class);
			
			conf.setOutputKeyClass(IntWritable.class);
			conf.setOutputValueClass(SetWritable.class);
			
			conf.setInputFormat(SequenceFileInputFormat.class);
			conf.setOutputFormat(SequenceFileOutputFormat.class);
			
			conf.setInputPath(new Path(input));
			conf.setOutputPath(outPath);

			conf.setMapperClass(RemoveNotFrequentMapper.class);

			if (param !=null)
				conf.setNumMapTasks(param.numOfMappers);
			
			conf.setNumReduceTasks(0);//TODO change it later
			

			RunningJob rj= JobClient.runJob(conf);
		} catch (Exception e) {
			e.printStackTrace();
		}
	}
	
	public static void main(String[] args){
		RemoveNotFrequentMapper.runJob("data/input", "data/input_removed","data/freqs", null);
	}
	

}
