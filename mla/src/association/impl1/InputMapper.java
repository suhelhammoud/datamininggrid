package association.impl1;

import java.io.IOException;

import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.mapred.JobClient;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.Mapper;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reporter;
import org.apache.hadoop.mapred.SequenceFileOutputFormat;
import org.apache.hadoop.mapred.TextInputFormat;

import utils.SetWritable;

public class InputMapper extends MapReduceBase implements Mapper<WritableComparable,Text,IntWritable,SetWritable> {

	/**
	 * input, data file lined
	 * line number + : + data
	 * outout
	 * sequence file
	 * IntWritable SetWritable
	 */
	public void map(WritableComparable key, Text values,
			OutputCollector<IntWritable,SetWritable> output, Reporter reporter) throws IOException {
		String[] arr=values.toString().split("[ ,:\t]");

		IntWritable line=new IntWritable(Integer.parseInt(arr[0]));
		SetWritable outset=new SetWritable();
		for (int i = 1; i < arr.length; i++) {
			outset.add(Integer.parseInt(arr[i]));
		}
		output.collect(line, outset);
	}

	public static void runJob(String src,String out, Param param){

		JobConf conf = new JobConf(InputMapper.class);
		conf.setJobName("InputMapper");
		
		conf.setOutputKeyClass(IntWritable.class);
		conf.setOutputValueClass(SetWritable.class);

		conf.setMapOutputKeyClass(IntWritable.class);
		conf.setMapOutputValueClass(SetWritable.class);

		conf.setInputFormat(TextInputFormat.class);
		conf.setOutputFormat(SequenceFileOutputFormat.class);

		conf.setInputPath(new Path(src));

		Path outpath=new Path(out);
		conf.setOutputPath(outpath);

		try {
			FileSystem fs = FileSystem.get(conf);
			if (fs.exists(outpath)) {
				fs.delete(outpath);
			}
		} catch (Exception e) {e.printStackTrace();
		}

		conf.setMapperClass(InputMapper.class);

		if ( param != null)
			conf.setNumMapTasks(param.numOfMappers);

		conf.setNumReduceTasks(0);

		//conf.setNumMapTasks(2);
		try {
			//JobClient client=new JobClient(conf);
			JobClient.runJob(conf);
		} catch (Exception e) {e.printStackTrace();

		}

	}

	public static void main(String[] args){
		InputMapper.runJob("data/input_lined", "data/input", null);
	}

}
