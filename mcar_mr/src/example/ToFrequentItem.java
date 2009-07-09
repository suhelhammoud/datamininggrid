package example;

import java.io.IOException;
import java.util.Iterator;
import java.util.List;

import mcar.mapreduce.DataBag;
import mcar.mapreduce.JobConf;
import mcar.mapreduce.MapReduceBase;
import mcar.mapreduce.Mapper;
import mcar.mapreduce.OutputCollector;
import mcar.mapreduce.Reducer;
import mcar.mapreduce.Reporter;

public class ToFrequentItem extends MapReduceBase implements
		Mapper<Integer, DataBag, DataBag, DataBag>,
		Reducer<DataBag, DataBag, DataBag, DataBag> {


	@Override
	public void map(Integer line, DataBag value,
			OutputCollector<DataBag, DataBag> output, Reporter reporter)
			throws IOException {

		DataBag classItem=(DataBag) value.get(value.size()-1);
		String classLabel=(String) classItem.get(1);
		
		DataBag outValue=new DataBag();
		outValue.add(classLabel);
		outValue.add(line);
		
		for (int i = 0; i < value.size(); i++) {
			DataBag outkey=new DataBag();
			outkey.add(value.get(i));
			output.collect(outkey, outValue);
		}
		
	}

	@Override
	public void reduce(DataBag key, Iterator<DataBag> values,
			OutputCollector<DataBag, DataBag> output, Reporter reporter)
			throws IOException {
		while(values.hasNext())
			System.out.println(values.next());
		
	}
	
	public static DataBag runJob(DataBag inputData, boolean verbos){
		JobConf job=new JobConf();
		job.setJobName("ToFrequentItem");
		job.setVerbose(verbos);
		job.setMapperClass(ToFrequentItem.class);
		job.setReducerClass(ToFrequentItem.class);
		job.set_input(inputData);
		return (DataBag)job.run();
		
	}
	
	public static void main(String[] args) {
		//initiaize data
		
	}

}
