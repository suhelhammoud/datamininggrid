package init;

import org.apache.log4j.Logger;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeSet;

 import javax.xml.transform.Result;

import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.mapred.Counters;
import org.apache.hadoop.mapred.FileInputFormat;
import org.apache.hadoop.mapred.FileOutputFormat;
import org.apache.hadoop.mapred.JobClient;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.Mapper;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reducer;
import org.apache.hadoop.mapred.Reporter;
import org.apache.hadoop.mapred.RunningJob;
import org.apache.hadoop.mapred.SequenceFileInputFormat;
import org.apache.hadoop.mapred.SequenceFileOutputFormat;


import others.MyConstants;
import others.MyCounter;
import others.MyConstants.COUNTERS;
import others.MyConstants.JOB;
import others.MyConstants.TAG;

import dataTypes.IntListWritable;
import dataTypes.IntMap;
import dataTypes.fia.Two;

public class ToItems extends MapReduceBase implements MyConstants,
Mapper<IntWritable, IntListWritable, IntListWritable, IntMap>,
Reducer<IntListWritable, IntMap, IntListWritable, IntMap> {
	/**
	 * Logger for this class
	 */
	private static final Logger logger = Logger.getLogger(ToItems.class);

	public int iteration = -1;
	public int label = 55;
	int support;
	float confidence;

	@Override
	public void configure(JobConf job) {
		label = job.getInt(JOB.label.name(), 66);
		iteration = job.getInt(JOB.iteration.name(), 65656565);
		support = job.getInt(JOB.support.name(), 1);
		confidence = job.getFloat(JOB.confidence.name(), 0.3f);
	}

	@Override
	public void map(IntWritable key, IntListWritable value,
			OutputCollector<IntListWritable, IntMap> output, Reporter reporter)
	throws IOException {

		int labelvalue=Integer.MAX_VALUE;
		List<IntListWritable> outList = new ArrayList<IntListWritable>((value.size()-1)/iteration);

		for (Iterator<Integer> iter = value.iterator(); iter.hasNext();) {
			int i=iter.next();
			if(i<0){
				labelvalue=-i;
				continue;
			}
			IntListWritable ruleId=new IntListWritable(iteration);
			ruleId.add(i);
			for (int j = 0; j < iteration-1; j++) {
				ruleId.add(iter.next());
			}
			outList.add(ruleId);
		}

		if(value.size()==1)return;
		//output the label
		{
			IntMap labelOutput=new IntMap();
			labelOutput.add(labelvalue, key.get());
			IntListWritable labelKeyOut=new IntListWritable(1);
			labelKeyOut.add(-labelvalue);
			output.collect(labelKeyOut, labelOutput);
		}
		//IntListWritable labelList = null;
		//int listSize = -value.get(value.size() - 1);

		// logger.info("label list " + labelList);

		IntMap outValue = new IntMap();
		outValue.add(labelvalue, key.get());

		logger.error(key+ ","+ value);

		ComposeGenerator cmpz=new ComposeGenerator(outList);
		while(cmpz.hasNext()){
			IntListWritable outItem=cmpz.next();
			//logger.error("first "+ outItem);
			output.collect(outItem, outValue);
		}


	}

	@Override
	public void reduce(IntListWritable key, Iterator<IntMap> values,
			OutputCollector<IntListWritable, IntMap> output, Reporter reporter)
	throws IOException {
		IntMap outValue = new IntMap();
		while (values.hasNext()) {
			IntMap m = values.next();
			outValue.addMap(m);
		}

		//TODO move it to after the label emit
		int[] tags = outValue.calc();

		//output label and exit
		if(key.get(0)<0){
			outValue.setIsRule(true);
			output.collect(key, outValue);
			return;
		}
		
		
		

		if (tags[TAG.support.ordinal()] >= support ) {
			reporter.incrCounter(COUNTERS.items_left, 1);

			key.remove(key.size() - 1);
			key.set(key.size() - 1, tags[TAG.minline.ordinal()]);
			
			//key.add(tags[TAG.minline.ordinal()]);

			if(tags[TAG.confidence.ordinal()] >= confidence){
				reporter.incrCounter(COUNTERS.candidate_rules, 1);
				outValue.setIsRule(true);
			}
			//logger.error(key+",,,,"+outValue);

			output.collect(key, outValue);
		}
	}

	public static IntListWritable isClose3(List<Integer> list1,
			List<Integer> list2) {

		int rint = 0;
		List<Integer> subList1 = list1.subList(0, list1.size() - 1);
		List<Integer> subList2 = list2.subList(0, list2.size() - 1);
		Set<Integer> resutl = new TreeSet<Integer>(subList1);

		for (Integer i : subList2) {
			if (resutl.add(i)) {
				rint++;
				if (rint > 1)
					return null;
			}
		}

		IntListWritable r = new IntListWritable(resutl);
		int i1 = list1.get(list1.size() - 1);
		int i2 = list2.get(list2.size() - 1);
		Collections.sort(r);
		r.add(Math.max(i1, i2));
		r.add(Math.min(i1, i2));
		return r;

	}

	public static long[] run(String linesDir,String itemsDir,int iteration,
			int label,int support,float confidence) throws IOException {
		JobConf job=new JobConf();
		job.setJobName(ToItems.class.getName());

		job.setInt(JOB.iteration.name(), iteration);
		job.setInt(JOB.label.name(), label);
		job.setInt(JOB.support.name(), support);
		job.set(JOB.confidence.name(),String.valueOf(confidence));

		linesDir = linesDir+"/"+ (iteration - 1);
		itemsDir = itemsDir + "/" +iteration;


		job.setOutputKeyClass(IntListWritable.class);
		job.setOutputValueClass(IntMap.class);

		job.setMapOutputKeyClass(IntListWritable.class);
		job.setMapOutputValueClass(IntMap.class);

		job.setMapperClass(ToItems.class);
		job.setCombinerClass(ToItemCombiner.class);
		job.setReducerClass(ToItems.class);

		job.setNumMapTasks(1);
		job.setNumReduceTasks(1);

		job.setInputFormat(SequenceFileInputFormat.class);
		job.setOutputFormat(SequenceFileOutputFormat.class);
		// job.setInputFormat(TextInputFormat.class);
		// job.setOutputFormat(TextOutputFormat.class);
		

		Path inPath = new Path(linesDir);
		Path outPath = new Path(itemsDir);

		FileSystem fs = FileSystem.get(job);
		fs.delete(outPath, true);
		FileInputFormat.setInputPaths(job, inPath);
		FileOutputFormat.setOutputPath(job, outPath);

		RunningJob rj = JobClient.runJob(job);
		Counters counters = rj.getCounters();
		return new long[] { counters.getCounter(COUNTERS.items_left),
				counters.getCounter(COUNTERS.candidate_rules) };
	}



	private static void testIsClose() {
		Integer[] arr = { 1, 2, 3, 4, 5 };
		Integer[] arr2 = { 1, 3, 2, 7, 8 };

		List<Integer> list = Arrays.asList(arr);
		List<Integer> list2 = Arrays.asList(arr2);
		System.out.println(list);
		System.out.println(list2);

		List<Integer> sub = list.subList(0, list.size() - 1);
		System.out.println(sub);
		System.out.println(isClose3(list, list2));
	}

	public static void main(String[] args) {
		List<IntListWritable> list=new ArrayList<IntListWritable>();
		IntListWritable l1=new IntListWritable();
		l1.add2(2, 4);
		IntListWritable l2=new IntListWritable();
		l2.add2(1, 4);
		list.add(l1);list.add(l2);
		System.out.println(list);
		ComposeGenerator cmpz=new ComposeGenerator(list);
		while(cmpz.hasNext()){
			System.out.println(cmpz.next());
		}
	}
}

class ToItemCombiner extends MapReduceBase implements
Reducer<IntListWritable, IntMap, IntListWritable, IntMap> {
	/**
	 * Logger for this class
	 */
	private static final Logger logger = Logger.getLogger(ToItemCombiner.class);

	@Override
	public void reduce(IntListWritable key, Iterator<IntMap> values,
			OutputCollector<IntListWritable, IntMap> output, Reporter reporter)
	throws IOException {
		IntMap outValue = new IntMap();
		while (values.hasNext()) {
			IntMap v = values.next();
			outValue.addMap(v);
		}
		//logger.error(key+","+outValue);
		output.collect(key, outValue);
	}

}

class ComposeGenerator implements Iterator<IntListWritable> {
	/**
	 * Logger for this class
	 */
	private static final Logger logger = Logger.getLogger(ComposeGenerator.class);

	MyCounter myCounter;
	List<IntListWritable> list;
	Set<Integer> hashSet;
	IntListWritable nextItem;

	public ComposeGenerator(List<IntListWritable> list) {
		this.list=list;
		this.myCounter=new MyCounter(list.size(),2);
		hashSet=new HashSet<Integer>((int)MyCounter.length(list.size(), 2));
		
		logger.error("new Compose "+list);
		advance();

	}

	private static int calcHash(List<Integer> list){
		int hsh = 1;

		for (int i :list) {
			hsh = 31*hsh + i;
		}
		return hsh;
	}

	public static IntListWritable isClose(List<Integer> list1,
			List<Integer> list2) {

		int rint = 0;
		int dex=list1.size() - 2;
		
		//
		int first1=list1.get(0);
		int first2=list2.get(0);
		
		if(first1==first2 || list1.get(dex)==list2.get(dex))
			return null;
		
		List<Integer> l1,l2;
		if(first1<first2){
			l1=list1;
			l2=list2;
		}else{
			l1=list2;
			l2=list1;
		}
		
		for (int i = 1; i <= dex; i++) {
			if(l1.get(i) != l2.get(i-1))
				return null;
		}
//		for (Integer i : subList2) {
//			if (resutl.add(i)) {
//				rint++;
//				if (rint > 1)
//					return null;
//			}
//		}
//		if(rint==0)return null;

		IntListWritable r = new IntListWritable(dex+3);
		for (int i = 0; i < l1.size()-1; i++) {
			r.add(l1.get(i));
		}
		r.add(l2.get(dex));
		
		int label1=l1.get(dex+1);
		int label2=l2.get(dex+1);
		
		//Collections.sort(r);
		r.add(label1);
		r.add(label2);
		return r;

	}

	@Override
	public boolean hasNext() {
		return nextItem !=null;
	}

	private void advance(){
		//logger.error("nextItem "+nextItem);
		while(myCounter.hasNext()){
			int[] index = myCounter.nextIndex();
			List<Integer> list1=list.get(index[0]);
			List<Integer> list2=list.get(index[1]);
			
			IntListWritable result=isClose(list1, list2);
			//logger.error("check "+ list1+"+"+ list2+"="+result);
			if(result == null )continue;
			int hashCode=calcHash(result);
			if( ! hashSet.contains(hashCode)){
				nextItem=result;
				hashSet.add(hashCode);
				return;
			}else{
				//logger.error("item exists before "+ result);
			}
		}
		nextItem=null;
	}
	@Override
	public IntListWritable next() {
		IntListWritable result=nextItem;
		advance();
		//logger.error("retrun "+nextItem);
		return result;

	}

	@Override
	public void remove() {
		// TODO Auto-generated method stub

	}
}