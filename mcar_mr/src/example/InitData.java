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
		for (int i = 1; i < item.length; i++) {
			DataBag v=new DataBag();
			v.add(i);
			v.add(item[i]);
			outvalue.add(v);
		}
		output.collect(key, outvalue);

	}
	
	
	public static DataBag runJob(){
		JobConf job=new JobConf();
		job.setMapperClass(InitData.class);
		job.set_input("data/example.txt");
		return job.run();
	}
	
	public static void main(String[] args) {
		DataBag output=runJob();
		for (Object item : output) {
			System.out.println(item);
		}
		
	}

}
