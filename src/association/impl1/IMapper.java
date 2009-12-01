package association.impl1;

import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.io.Text;
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
import org.apache.log4j.Logger;

import utils.MapTools;
import utils.SetWritable;

import com.sun.org.apache.bcel.internal.generic.FMUL;
import com.sun.org.apache.regexp.internal.REProgram;


import java.awt.image.BufferStrategy;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

public class IMapper extends MapReduceBase implements
Mapper<IntWritable, SetWritable, SetWritable, IntWritable> {

	static Logger log=Logger.getLogger(IMapper.class);
	
	private ItemMap fMap;

	private static final IntWritable one=new IntWritable(1);
	public void map(IntWritable key, SetWritable values,
			OutputCollector<SetWritable, IntWritable> output, Reporter reporter) throws IOException {
		//log.info("\nline: "+values.toString());
		
		long tic=D.tic();
		List<SetWritable> list=fMap.getSubsets(values, 0, fMap.ITERATION);
		long toc=D.toc(tic);
		reporter.incrCounter(Count.count.TIME_FOR_SUBSETS, toc);
		reporter.incrCounter(Count.count.Num_OF_SUBSETS, list.size());
		
		tic=D.tic();
		for (SetWritable sw : list) {
			output.collect(sw, one);
		}
		reporter.incrCounter(Count.count.TIME_TO_COLLECT, D.toc(tic));
	}
	

	@Override
	public void configure(JobConf conf) {
		super.configure(conf);
		fMap=new ItemMap();
		fMap.configure(conf);
		fMap.load(conf);
		//log.info("map "+fMap.toString());
	}

	public static int runJob(String input,String freqDir, int iteration,int support, Param param){
		try {
			JobConf conf = new JobConf(IMapper.class);
			conf.setJobName("IMapper "+iteration);
			conf.setInt("ITERATION", iteration);
			conf.setInt("SUPPORT", support);
			conf.set("freqDir", freqDir);

			Path outPath = new Path(freqDir+"/"+iteration);
			FileSystem fs = FileSystem.get(conf);
			//TODO remove later,
			if (fs.exists(outPath)) {
				fs.delete(outPath);
			}

			conf.setOutputKeyClass(SetWritable.class);
			conf.setOutputValueClass(IntWritable.class);

			conf.setInputPath(new Path(input));
			conf.setOutputPath(outPath);

			conf.setMapperClass(IMapper.class);
			conf.setCombinerClass(ICombiner.class);
			conf.setReducerClass(IReducer.class);
			
			conf.setOutputFormat(SequenceFileOutputFormat.class);
			conf.setInputFormat(SequenceFileInputFormat.class);

			if (param != null){
				conf.setJobName(param.jobName +" IMapper "+iteration);
				
				conf.setNumMapTasks(param.numOfMappers);
				conf.setNumReduceTasks(param.numOfReduces);//TODO change it later
			}
			
			RunningJob rj= JobClient.runJob(conf);
			Counters counters=rj.getCounters();

			long c=counters.getCounter(Count.count.Items_LEFT );

			return (int)c;


		} catch (Exception e) {
			e.printStackTrace();
			return -1;
		}		
		
	}

}

