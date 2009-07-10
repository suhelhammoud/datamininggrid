package example;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import mcar.mapreduce.*;

public class InitData extends MapReduceBase implements
		Mapper<Integer, String,Integer, DataBag> {

	@Override
	public void map(Integer key, String value,
			OutputCollector<Integer, DataBag> output, Reporter reporter)
			throws IOException {
		String[] item=value.split(",");
		
		
		DataBag outvalue=new DataBag();
		for (int i = 1; i < item.length-1; i++) {
			DataBag v=new DataBag();
			v.add(i-1);
			v.add(Integer.valueOf(item[i]));
			outvalue.add(v);
		}
		//init classBag with one item
		DataBag classBag=new DataBag();
		classBag.add(Integer.valueOf(item[item.length-1]));
		outvalue.add(classBag);
		
		output.collect(key, outvalue);

	}
	
	
	public static DataBag runJob(String fileName, boolean verbos){
		JobConf job=new JobConf();
		job.setJobName(InitData.class.getName());
		
		job.setVerbose(verbos);
		job.setMapperClass(InitData.class);
		job.set_input(fileName);
		return job.run();
	}
	
	public static void main(String[] args) {
		DataBag output=runJob("data/example.txt",true);
		for (Object item : output) {
			System.out.println(item);
		}
		
	}

}
