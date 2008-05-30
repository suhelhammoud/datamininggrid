package association.impl1;

import java.io.IOException;
import java.util.Iterator;

import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.mapred.JobClient;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.Mapper;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reducer;
import org.apache.hadoop.mapred.Reporter;
import org.apache.hadoop.mapred.SequenceFileInputFormat;
import org.apache.hadoop.mapred.SequenceFileOutputFormat;

import utils.SetWritable;


public class RSortMapper extends MapReduceBase implements Mapper<Rl,IntWritable,Rl,IntWritable> {

	public void map(Rl key, IntWritable values,
			OutputCollector<Rl,IntWritable> output, Reporter reporter) throws IOException {
		output.collect(key, values);
	}

	public static void runJob(String input,String output, Param param){
		try {		
			JobConf conf=new JobConf(RSortMapper.class);
			conf.setJobName("RSortMaooer");
			
			Path outPath = new Path(output);
			FileSystem fs = FileSystem.get(conf);

			//TODO remove later,
			if (fs.exists(outPath)) {
				fs.delete(outPath);
			}
			conf.setInputPath(new Path(input));
			conf.setOutputPath(outPath);

			conf.setOutputKeyClass(Rl.class);
			conf.setOutputValueClass(IntWritable.class);

			conf.setInputFormat(SequenceFileInputFormat.class);
			conf.setOutputFormat(SequenceFileOutputFormat.class);

			conf.setMapOutputKeyClass(Rl.class);
			conf.setMapOutputValueClass(IntWritable.class);

			conf.setMapperClass(RSortMapper.class);
			conf.setReducerClass(RSortReducer.class);

			if (param != null){
				conf.setNumMapTasks(param.numOfMappers);
				conf.setNumReduceTasks(param.numOfReduces);
			}
			JobClient.runJob(conf);


		} catch (Exception e) {
			e.printStackTrace();
		}

	}

	public static void main(String[] args){
		RSortMapper.runJob("data/rules", "data/rules_ranked", null);
	}

}

class RSortReducer extends MapReduceBase implements Reducer<Rl,IntWritable,Rl,IntWritable> {

	public void reduce(Rl key, Iterator<IntWritable> values,
			OutputCollector<Rl,IntWritable> output, Reporter reporter) throws IOException {
		while(values.hasNext())
			output.collect(key, values.next());
	}
}
