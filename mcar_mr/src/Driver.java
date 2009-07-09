

import java.io.IOException;
import java.util.Iterator;
import java.util.List;

import mcar.mapreduce.*;

public class Driver extends MapReduceBase  implements
		Mapper<Integer, String, String, Integer>
		 {

	enum c {line};
	
	@Override
	public void map(Integer key, String value,
			OutputCollector<String, Integer> output, Reporter reporter)
			throws IOException {
		//System.out.println(key + "," + value);
		String[] v = value.split(",");
		for (String s : v) {
			output.collect(s, 1);
		}
		reporter.incrCounter(c.line, 1);
	}

	

	
	
	@Override
	public void configure(JobConf job) {
		System.out.println("test "+job.get("suhel"));
	}
	
	public static void main(String[] args) {
		JobConf job = new JobConf();
		job.set("suhel", "hammoud");
		job.set_input("data/raw.txt");
		job.setMapperClass(Driver.class);
		job.setReducerClass(MR.class);

		List<KeyValue> result = job.run();
		for (KeyValue kv : result) {
			System.out.println(kv);
		}
		
		try {
			Counters counters=job.getCounters();
			System.out.println(counters);
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}

	}
	static public class MR extends MapReduceBase implements 
	Reducer<String, Integer, String, Integer>{
		
		@Override
		public void reduce(String key, Iterator<Integer> values,
				OutputCollector<String, Integer> output, Reporter reporter)
				throws IOException {
			int sum=0;
			while(values.hasNext()){
				sum+=values.next();
			}
			output.collect(key, new Integer(sum));
		}
		
		@Override
		public void configure(JobConf job) {
			System.out.println("configure reduce");
		}
	}
}

