package association.impl1;

import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
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


import java.io.IOException;

public class ZeroMapper extends MapReduceBase implements
Mapper<IntWritable, SetWritable, IntWritable, IntWritable> {


	private IntWritable k=new IntWritable();
	private IntWritable one=new IntWritable(1);
	
	public void map(IntWritable key, SetWritable values,
			OutputCollector<IntWritable, IntWritable> output, Reporter reporter) throws IOException {

		reporter.incrCounter(Count.count .NUMBER_OF_ROWS , 1);

		for (Integer atomItem : values) {
			k.set(atomItem);
			output.collect(k, one);
		}
	}
	
	public static int runJob(String input,String output, int support, Param param){
		try {
			JobConf conf=new JobConf(ZeroMapper.class);
			conf.setJobName("ZeroMapper");
			
			conf.setInt("SUPPORT", support);
			
			Path outPath = new Path(output);
			FileSystem fs = FileSystem.get(conf);
			if (fs.exists(outPath)) {
				fs.delete(outPath);
			}

			conf.setMapOutputKeyClass(IntWritable.class);
			conf.setMapOutputValueClass(IntWritable.class);
			
			conf.setOutputKeyClass(SetWritable.class);
			conf.setOutputValueClass(IntWritable.class);
			
			conf.setInputFormat(SequenceFileInputFormat.class);
			conf.setOutputFormat(SequenceFileOutputFormat.class);
			
			conf.setInputPath(new Path(input));
			conf.setOutputPath(outPath);

			conf.setMapperClass(ZeroMapper.class);
			conf.setCombinerClass(ZeroCombiner.class);
			conf.setReducerClass(ZeroReducer.class);

			if(param != null){
				conf.setJobName(param.jobName +" ZeroMapper");

				conf.setNumMapTasks(param.numOfMappers);
				conf.setNumReduceTasks(param.numOfReduces);//TODO change it later
				
			}
			

			RunningJob rj= JobClient.runJob(conf);

			Counters counters=rj.getCounters();
			long c=counters.getCounter(Count.count.NUMBER_OF_ROWS );
			System.out.println("Counters "+c );


			long itemsLeft=counters.getCounter(Count.count.Items_LEFT );
			System.out.println("rows left "+itemsLeft );


			return (int)itemsLeft;
		} catch (Exception e) {
			e.printStackTrace();
			return -1;
		}
	}
	
	public static void main(String[] args){
		System.out.println("ItemsLeft"+ ZeroMapper.runJob("data/input", "data/freqs/1", 0, null));
	}

}
