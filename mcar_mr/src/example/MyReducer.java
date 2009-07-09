package example;

import java.io.IOException;
import java.util.Iterator;
import mcar.mapreduce.*;

public class MyReducer extends MapReduceBase implements Reducer<String, Integer, String, Integer>{
	
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
}
