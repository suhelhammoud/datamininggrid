package example;

import java.io.IOException;
import java.util.Iterator;

import mcar.mapreduce.DataBag;
import mcar.mapreduce.JobConf;
import mcar.mapreduce.MapReduceBase;
import mcar.mapreduce.Mapper;
import mcar.mapreduce.OutputCollector;
import mcar.mapreduce.Reducer;
import mcar.mapreduce.Reporter;

public class ToItemSizeOne extends MapReduceBase implements
Mapper<Integer, DataBag, DataBag, DataBag>,
Reducer<DataBag, DataBag, DataBag, FrequentItem> {

	
	//public int classIndex = 55;
	int support;
	int confidence;

	@Override
	public void configure(JobConf job) {
		//classIndex = job.getInt("classIndex", 66);
		support = job.getInt("support", 1);
		confidence = job.getInt("confidence", 0);
	}

	@Override
	public void map(Integer key, DataBag value,
			OutputCollector<DataBag, DataBag> output, Reporter reporter)
			throws IOException {
		DataBag classItem=(DataBag) value.get(value.size()-1);
		Integer classLabel=(Integer) classItem.get(0);
		
		DataBag outValue=new DataBag();
		outValue.add(classLabel);
		outValue.add(key);
		
		for (int i = 0; i < value.size(); i++) {
			output.collect((DataBag)value.get(i), outValue);
		}		
	}

	@Override
	public void reduce(DataBag key, Iterator<DataBag> values,
			OutputCollector<DataBag, FrequentItem> output, Reporter reporter)
			throws IOException {
		FrequentItem fi=new FrequentItem();
		while(values.hasNext()){
			DataBag db=values.next();
			fi.add((Integer)db.get(0), (Integer)db.get(1));
		}
		int[] tags = fi.calc();
		
		//if class output and exit
		if(key.size()==1){
			//fi.setIsRule(true);
			output.collect(key, fi);
			return;
		}
		
		if (fi.getSupport() >= support ) {
			reporter.incrCounter("my","counterOne", 1);

//			key.remove(key.size() - 1);
//			key.set(key.size() - 1, fi.minLine);
			
			//key.add(tags[TAG.minline.ordinal()]);

			if(fi.getConfidene() >= confidence){
				reporter.incrCounter("my","cRules", 1);
				fi.setIsRule(true);
			}
			//logger.error(key+",,,,"+outValue);

			output.collect(key, fi);
		}
	}
	
	public static DataBag runJob(DataBag inputData, 
			int support, int confidence, boolean verbos){
		JobConf job=new JobConf();
		job.setJobName(ToItemSizeOne.class.getName());

		job.setInt("support", support);
		job.setInt("confidence",confidence);

	
		
		job.setVerbose(verbos);
		job.setMapperClass(ToItemSizeOne.class);
		job.setReducerClass(ToItemSizeOne.class);
		job.set_input(inputData);
		return (DataBag)job.run();
		
	}
	
	public static void main(String[] args) {
		//initiaize data
		
		int support =2;
		int confidence= (int)(0.4 * Integer.MAX_VALUE);
		DataBag dataReady=InitData.runJob("data/example.txt",false);
		for (Object i : dataReady) {
			System.out.println(i);
		}
		

		//one step in finding frequent items sets
		
		// map data to FrequentItem space
		DataBag dataout=ToItemSizeOne.runJob(dataReady,support,confidence,true);
		
		// map data to Line space
	
		
		for (Object i : dataout) {
			System.out.println(i);
		}
		
		DataBag out2=ToLines.runJob(dataout, true);
		for (Object object : out2) {
			System.out.println(object);
		}
	}


}
