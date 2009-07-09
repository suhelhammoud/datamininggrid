package example;

import java.io.IOException;
import java.util.Iterator;
import java.util.List;

import mcar.mapreduce.*;

/** <Integer key1, String value1> ---> Mapper ---<String key2, Integer value2>	*/
public class WordCounter extends MapReduceBase  implements 
Mapper<Integer, String, String, Integer>,
Reducer<String, Integer, String, Integer>{

	/**
	 * map method get the inputs as <key,value> of typs, process it, then emit <
	 */
	@Override
	public void map(Integer key, String value,
			OutputCollector<String, Integer> output, Reporter reporter)
			throws IOException {
		
		String[] words = value.split(",");
		for (int i = 1; i < words.length; i++) {
			output.collect(words[i], 1);
		}
	}
	
	@Override
	public void reduce(String key, Iterator<Integer> values,
			OutputCollector<String, Integer> output, Reporter reporter)
			throws IOException {
		int sum=0;
		while(values.hasNext()){//iterate through the values (all are with same key)
			sum+=values.next();
		}
		output.collect(key, new Integer(sum));
	}
	public static void main(String[] args) {
		JobConf job=new JobConf();
		
		job.set_input("data/example.txt");
		job.setMapperClass(WordCounter.class);
		job.setReducerClass(WordCounter.class);
		
		//run the job and get the data
		List<KeyValue> outData=job.run();
		
		for (KeyValue entry : outData) {
			System.out.println(entry);
		}
	}
}
