package association.impl1;


import java.io.IOException;

import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.mapred.JobClient;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.Mapper;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reporter;
import org.apache.hadoop.mapred.RunningJob;
import org.apache.hadoop.mapred.SequenceFileInputFormat;
import org.apache.hadoop.mapred.SequenceFileOutputFormat;
import org.apache.log4j.Logger;

import utils.PowerSet2;
import utils.SetWritable;


public class RMapper extends MapReduceBase implements
Mapper<SetWritable, IntWritable, SetWritable, ItemFreq> {

	static Logger log=Logger.getLogger(RMapper.class);
	
	public void map(SetWritable key, IntWritable values,
			OutputCollector<SetWritable,ItemFreq> output, Reporter reporter) throws IOException {


		//throw empty ItemFreq "":freq
		ItemFreq itmFreq=new ItemFreq(new SetWritable(),values);
		output.collect(key, itmFreq);
		//log.info("collect:"+key+ "-->"+itmFreq );

		if(key.size()==1 ) return;
		PowerSet2 ps=new PowerSet2(key);		
		for (SetWritable sw : ps) {
			sw=new SetWritable(sw);
			SetWritable csw=Rl.complement(key, sw);
			ItemFreq ifrq=new ItemFreq(csw,values);
			output.collect(sw,ifrq );
			//log.info("collect:"+sw+ "-->"+ifrq );
		}

	}

	public static void runJob(String input,String output,int total,float confidence, Param param){
		try {
			JobConf conf = new JobConf(RMapper.class);
			conf.setJobName("RMapper");
			
			conf.setInt("TOTAL", total);
			conf.set("CONFIDENCE", String.valueOf(confidence));

			Path outPath = new Path(output);
			FileSystem fs = FileSystem.get(conf);
			//TODO remove later,
			if (fs.exists(outPath)) {
				fs.delete(outPath);
			}

			conf.setMapOutputKeyClass(SetWritable.class);
			conf.setMapOutputValueClass(ItemFreq.class);

			conf.setOutputKeyClass(Rl.class);
			conf.setOutputValueClass(IntWritable.class);

			conf.setInputPath(new Path(input));
			conf.setOutputPath(outPath);

			conf.setMapperClass(RMapper.class);
			conf.setReducerClass(RReducer.class);

			conf.setOutputFormat(SequenceFileOutputFormat.class);
			conf.setInputFormat(SequenceFileInputFormat.class);

			if (param != null){
				conf.setNumMapTasks(param.numOfMappers);
				conf.setNumReduceTasks(param.numOfReduces);
			}
//			conf.setNumReduceTasks(1);//TODO change it later

			RunningJob rj= JobClient.runJob(conf);



		} catch (Exception e) {
			e.printStackTrace();
		}		

	}
	
	public static void main(String[] args){
		RMapper.runJob("data/freqs/*", "data/rules", 9, 0.2f, null);
	}

}
