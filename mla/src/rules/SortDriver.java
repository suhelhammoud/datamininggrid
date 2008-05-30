package rules;

import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.JobClient;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.SequenceFileInputFormat;
import org.apache.hadoop.mapred.TextOutputFormat;

import sun.security.krb5.internal.SeqNumber;

public class SortDriver {

	public static void main(String[] args) {
		sort();
	}

	public static void sort() {
		JobClient client = new JobClient();
		JobConf conf = new JobConf(rules.SortDriver.class);
		
		conf.setInputFormat(SequenceFileInputFormat.class);
		

		conf.setMapperClass(rules.SortMapper.class);
		conf.setReducerClass(rules.SortReducer.class);
		
		conf.setMapOutputKeyClass(FloatWritable.class);
		conf.setMapOutputValueClass(Text.class);
		// TODO: specify output types
		conf.setOutputKeyClass(FloatWritable.class);
		conf.setOutputValueClass(Text.class);
		conf.setOutputFormat(TextOutputFormat.class);

		// TODO: specify input and output DIRECTORIES (not files)
		conf.setInputPath(new Path("data/rules/*"));		
		Path outpath=new Path("data/sorted");
		conf.setOutputPath(outpath);

		try {
			FileSystem fs = FileSystem.get(conf);
			if (fs.exists(outpath)) {
				fs.delete(outpath);
			}
		} catch (Exception e) {
			// TODO: handle exception
		}




		client.setConf(conf);
		try {
			JobClient.runJob(conf);
		} catch (Exception e) {
			e.printStackTrace();
		}
	}

}
