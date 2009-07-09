package old;



import java.io.IOException;
import java.util.List;

import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;

import org.apache.hadoop.mapred.Counters;
import org.apache.hadoop.mapred.FileInputFormat;
import org.apache.hadoop.mapred.FileOutputFormat;
import org.apache.hadoop.mapred.JobClient;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.Mapper;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reporter;
import org.apache.hadoop.mapred.RunningJob;

import org.apache.hadoop.mapred.TextInputFormat;
import org.apache.hadoop.mapred.TextOutputFormat;
import org.apache.hadoop.mapred.lib.IdentityMapper;
import org.apache.hadoop.mapred.lib.IdentityReducer;

import others.MyConstants;
import others.Tools;
import others.MyConstants.COUNTERS;


public class MyEmailPacker {

	public static void PackEmails(String inputDir,String outputDir) throws IOException{
		JobConf job=new JobConf();
		job.setJobName(MyEmailPacker.class.getName());

		job.setOutputKeyClass(LongWritable.class);
		job.setOutputValueClass(Text.class);

		job.setMapOutputKeyClass(LongWritable.class);
		job.setMapOutputValueClass(Text.class);

		job.setMapperClass(IdentityMapper.class);
		job.setReducerClass(IdentityReducer.class);

		job.setNumReduceTasks(1);//


		job.setInputFormat(TextInputFormat.class);
		job.setOutputFormat(TextOutputFormat.class);


		Path inPath = new Path(inputDir);
		Path outPath = new Path(outputDir);

		FileSystem fs = FileSystem.get(job);
		fs.delete(outPath, true);
		FileInputFormat.setInputPaths(job, inPath);

		FileOutputFormat.setOutputPath(job, outPath);

		RunningJob rj=JobClient.runJob(job);


	}

	public static void test(){
		List<Path> list=Tools.listAllFiles(new Path("data/in/flat"));
		System.out.println("number of input files "+ list.size());
		try {
			long t=System.nanoTime();

			MyMapper.run("data/in/flat", "data/in/out1");
			System.out.println("time1="+(System.nanoTime()-t));

			t=System.nanoTime();
			MyMapper.run("data/in/outtest", "data/in/out2");
			System.out.println("time1="+(System.nanoTime()-t));

		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}

	}
	public static void main(String[] args) throws IOException {
	
		
		//test();
		//		try {
		//			PackEmails("data/in/flat", "data/in/outtest");
		//		} catch (IOException e) {
		//			// TODO Auto-generated catch block
		//			e.printStackTrace();
		//		}
	}

}

class MyMapper extends MapReduceBase implements MyConstants,
Mapper<LongWritable, Text, LongWritable, Text> {

	@Override
	public void map(LongWritable key, Text value,
			OutputCollector<LongWritable, Text> output, Reporter reporter)
	throws IOException {
		reporter.incrCounter(MyConstants.COUNTERS.numbers_of_rows,1);
	}



	public static long run(String inputDir,String outputDir) throws IOException{
		JobConf job=new JobConf();
		job.setJobName("MyMapper");

		job.setOutputKeyClass(LongWritable.class);
		job.setOutputValueClass(Text.class);

		job.setMapOutputKeyClass(LongWritable.class);
		job.setMapOutputValueClass(Text.class);

		job.setMapperClass(MyMapper.class);

		job.setNumMapTasks(1);
		job.setInputFormat(TextInputFormat.class);
		job.setOutputFormat(TextOutputFormat.class);


		Path inPath = new Path(inputDir);
		Path outPath = new Path(outputDir);

		FileSystem fs = FileSystem.get(job);
		fs.delete(outPath, true);
		FileInputFormat.setInputPaths(job, inPath);
		FileOutputFormat.setOutputPath(job, outPath);

		RunningJob rj=JobClient.runJob(job);
		Counters counters=rj.getCounters();

		System.out.println(counters);
		return counters.getCounter(MyConstants.COUNTERS.numbers_of_rows);
	}


}
