package rules;

import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.JobClient;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.SequenceFileInputFormat;
import org.apache.hadoop.mapred.SequenceFileOutputFormat;
import org.apache.hadoop.mapred.TextOutputFormat;

public class RulesDriver {

	public static void main(String[] args) {
		gerRules(9);
		SortDriver.sort();
	}

	public static void gerRules(int total) {
		JobClient client = new JobClient();
		JobConf conf = new JobConf(rules.RulesDriver.class);
		//Set total size
		conf.setInt("TOTAL", total);
		conf.set("CONFIDENCE", String.valueOf(0.490));

		conf.setInputFormat(SequenceFileInputFormat.class);
		conf.setOutputFormat(SequenceFileOutputFormat.class);
		// TODO: specify output types
		conf.setMapOutputKeyClass(Text.class);
		conf.setMapOutputKeyClass(Text.class);

		conf.setOutputKeyClass(FloatWritable.class);
		conf.setOutputValueClass(Text.class);

		// TODO: specify input and output DIRECTORIES (not files)
		
		conf.setInputPath(new Path("data/freqs/*"));
		Path outpath=new Path("data/rules");
		conf.setOutputPath(outpath);

		try {
			FileSystem fs = FileSystem.get(conf);
			if (fs.exists(outpath)) {
				fs.delete(outpath);
			}
		} catch (Exception e) {
			// TODO: handle exception
		}

		// TODO: specify a mapper
		conf.setMapperClass(RulesMapper.class);

		// TODO: specify a combiner

		// TODO: specify a reducer
		conf.setReducerClass(RulesReducer.class);

		client.setConf(conf);
		try {
			JobClient.runJob(conf);
		} catch (Exception e) {
			e.printStackTrace();
		}
	}

}
