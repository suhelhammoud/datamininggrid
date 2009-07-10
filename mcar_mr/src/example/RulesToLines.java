package example;

import java.io.IOException;
import java.util.Iterator;
import java.util.List;

import mcar.mapreduce.*;



public class RulesToLines extends MapReduceBase  implements
Mapper<DataBag,FrequentItem,Integer,DataBag>,
Reducer<Integer, DataBag, Integer, DataBag> {

	@Override
	public void map(DataBag key, FrequentItem value,
			OutputCollector<Integer, DataBag> output, Reporter reporter)
	throws IOException {


		//if  class emit and return
		if( key.size()==1){
			for(List<Integer> list: value.values()){
				for (Integer i : list) {
					output.collect(i, key);
				}
			}
			return;
		}
		
		//if not rule return
		if(! value.isRule())return;
		
		// add support and confidence to the header of the databag
		key.add(0,value.getSupport());
		key.add(0,value.getConfidene());
		
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
		DataBag classBag=null;
		DataBag bestRule=null;
		
		while(values.hasNext()){
			DataBag rule=values.next();
			if(rule.size()==1){
				classBag=rule;//TODO check if need to use new DataBag(rule)
				return;
			}
			if(compareTwoRules(bestRule, rule)<0){
				bestRule=rule;//TODO check if need to use new DataBag(rule)
			}
		}
		
		output.collect(key, bestRule);
		
	}
	
	static int compareTwoRules(DataBag list1,DataBag list2){
		int confidence1=(Integer)list1.get(0);
		int confidence2=(Integer)list2.get(0);
		int support1=(Integer)list1.get(1);
		int support2=(Integer)list2.get(1);
		
		int dif=confidence1-confidence2;
		if(dif != 0)return dif;

		//support
		dif= support1-support2;
		if(dif != 0)return dif;
		
		//column length
		dif=list1.size()-list2.size();
		if(dif !=0) return -dif;//minus
		
		
		for (int i = 2; i < list1.size() && i<list2.size(); i++) {
			dif=(Integer)list1.get(i)-(Integer)list2.get(i);
			if(dif!=0)return dif;
		}

		//this is error case
		//logger.error("equal case "+ list1+", "+ list2);
		return 0;
		
	}
	
	public static DataBag runJob(DataBag inputData, boolean verbos){
		JobConf job=new JobConf();
		job.setJobName(RulesToLines.class.getName());
		job.setVerbose(verbos);
		job.setMapperClass(RulesToLines.class);
		//job.setReducerClass(RulesToLines.class);
		job.set_input(inputData);
		return (DataBag)job.run();
		
	}

}