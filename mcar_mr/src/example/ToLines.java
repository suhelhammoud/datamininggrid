package example;

import java.io.IOException;
import java.util.Iterator;
import java.util.List;

import mcar.mapreduce.*;


public class ToLines extends MapReduceBase implements 
Mapper<DataBag, FrequentItem, Integer, DataBag>,
Reducer<Integer, DataBag, Integer, DataBag> {



	@Override
	public void map(DataBag key, FrequentItem value,
			OutputCollector<Integer, DataBag> output, Reporter reporter)
	throws IOException {
		
		for(List<Integer> list: value.values()){
			for (Integer i : list) {
				output.collect(i, key);
			}
		}

	}


	@Override
	public void reduce(Integer key, Iterator<DataBag> values,
			OutputCollector<Integer, DataBag> output, Reporter reporter)
	throws IOException {
		DataBag db=new DataBag();
		DataBag outValue=new DataBag();
		DataBag classBag=null;
		while(values.hasNext()){
			DataBag d=values.next();
			if(d.size()==1){
				classBag=d;
				continue;
			}
			outValue.add(d);
		}
		if(outValue.size()==0)return;
		
		outValue.add(classBag);
		output.collect(key, outValue);
	}
	
	public static DataBag runJob(DataBag inputData, boolean verbos){
		JobConf job=new JobConf();
		job.setJobName("ToLines");
		//job.setInt("classIndex", classIndex);
		job.setVerbose(verbos);
		job.setMapperClass(ToLines.class);
		job.setReducerClass(ToLines.class);
		job.set_input(inputData);
		return (DataBag)job.run();
		
	}
	
	public static void main(String[] args) {
		int support =2;
		int confidence= (int)(0.4 * Integer.MAX_VALUE);
		
		DataBag dataReady=InitData.runJob("data/example.txt",false);
		for (Object i : dataReady) {
			System.out.println(i);
		}
		

		//one step in finding frequent items sets
		
		// map data to FrequentItem space
		DataBag dataout=ToItemSizeOne.runJob(dataReady,0,0,true);
		
		// map data to Line space
		
		System.out.println("dataout size "+ dataout.size());
		
		for (Object i : dataout) {
			System.out.println(i);
		}
		
		DataBag outToLine= ToLines.runJob(dataout, true);
		for (Object object : outToLine) {
			System.out.println(object);
		}
		
		DataBag outToItems=ToItems.runJob(outToLine, support, confidence, true);
	}
	
}