package example;

import mcar.mapreduce.*;


import java.io.IOException;
import java.util.*;

import javax.xml.crypto.Data;

public class ToItems extends MapReduceBase implements
		Mapper<Integer, DataBag, DataBag, DataBag>,
		Reducer<DataBag, DataBag, DataBag, FrequentItem> {
	/**
	 * Logger for this class
	 */
	private static final Logger logger = Logger.getLogger(ToItems.class);

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

		if(value.size()<3)return;
		
		DataBag classItem = (DataBag) value.get(value.size() - 1);
		Integer classLabel = (Integer) classItem.get(0);

		DataBag outValue = new DataBag();
		outValue.add(classLabel);
		outValue.add(key);

		DataBag outKeys = compose(value);
		for (int i = 0; i < outKeys.size(); i++) {
			output.collect((DataBag) outKeys.get(i), outValue);
		}

	}

	@Override
	public void reduce(DataBag key, Iterator<DataBag> values,
			OutputCollector<DataBag, FrequentItem> output, Reporter reporter)
			throws IOException {
		FrequentItem fi = new FrequentItem();
		while (values.hasNext()) {
			DataBag db = values.next();
			fi.add((Integer) db.get(0), (Integer) db.get(1));
		}

		// if class output and exit
		if (key.size() == 1) {
			fi.setIsRule(true);
			output.collect(key, fi);
			return;
		}

		fi.calc();//important to calc support confidence and minline
		
		if (fi.getSupport() >= support) {
			reporter.incrCounter("my", "counterOne", 1);

			key.remove(key.size() - 1);
			key.set(key.size() - 1, fi.minLine);

			// key.add(tags[TAG.minline.ordinal()]);

			if (fi.getConfidene() >= confidence) {
				reporter.incrCounter("my", "cRules", 1);
				fi.setIsRule(true);
			}
			// logger.error(key+",,,,"+outValue);

			output.collect(key, fi);
		}
	}

	public static DataBag runJob(DataBag inputData, 
			int support, int confidence,  boolean verbos) {
		JobConf job = new JobConf();
		job.setJobName(ToItems.class.getName());

		job.setInt("support", support);
		job.setInt("confidence", confidence);

		job.setMapperClass(ToItems.class);
		job.setReducerClass(ToItems.class);

		job.setVerbose(verbos);

		job.set_input(inputData);

		return job.run();

	}

	public static void main(String[] args) {
		int support =2;
		int confidence= (int)(0.4*Integer.MAX_VALUE );
		
		DataBag data=InitData.runJob("data/example.txt",false);
		
		//one step in finding frequent items sets
		// map data to FrequentItem space
		DataBag items=ToItemSizeOne.runJob(data,support,confidence,false);
		//System.out.println("iteration 1 size "+ items.size());
		
		DataBag lines= ToLines.runJob(items, false);//line 1
		
	
		
		
		items= ToItems.runJob(lines, support, confidence, false);//size 2
		
		

		System.out.println("iteration 2 size "+ items.size());
		lines=ToLines.runJob(items, true);
		for (Object object : lines) {
			System.out.println(object);
		}
		

		items= ToItems.runJob(lines, support, confidence, true);
		
		for (Object object : items) {
			System.out.println(object);
		}
		if(true)return;

		System.out.println("iteration 3 size "+ items.size());
		lines=ToLines.runJob(items, true);
		
		for (Object object : items) {
			System.out.println(object);
		}

		
		if(true)return;

		
	}

	public static DataBag composeRules(DataBag line) {
		DataBag result = new DataBag();

		TreeMap<DataBag, DataBag> left = new TreeMap<DataBag, DataBag>();
		TreeMap<DataBag, DataBag> right = new TreeMap<DataBag, DataBag>();

		for (int i = 0; i < line.size(); i++) {
			DataBag tmp = new DataBag((DataBag) line.get(i));
			DataBag coreLeft = new DataBag(tmp);
			coreLeft.remove(coreLeft.size() - 1);
			DataBag coreRight = new DataBag(tmp);
			coreRight.remove(0);

			DataBag leftList = left.get(coreLeft);
			if (leftList == null)
				leftList = new DataBag();
			leftList.add(tmp);
			left.put(coreLeft, leftList);

			DataBag rightList = right.get(coreRight);
			if (rightList == null)
				rightList = new DataBag();
			rightList.add(tmp);
			right.put(coreRight, rightList);
		}
		// System.out.println("left "+left);
		// System.out.println("right "+right);

		left.remove(left.firstKey());
		right.remove(right.lastKey());

		for (Map.Entry<DataBag, DataBag> e : right.entrySet()) {

			DataBag start = e.getValue();

			DataBag stop = left.get(e.getKey());
			if (stop == null)
				continue;

			// System.out.println("start "+ start);
			// System.out.println("stop "+ stop);
			for (Object o1 : start) {
				DataBag is1 = (DataBag) o1;
				for (Object o2 : stop) {
					DataBag is2 = (DataBag) o2;
					DataBag compound = new DataBag(is1);
					compound.add(is2.get(is2.size() - 1));
					result.add(compound);
					// System.out.println("join "+ is1+"\t"+is2
					// +"=\t"+ compound);
				}
			}
		}
		return result;

	}

	public static DataBag compose(DataBag line) {

		int sz = ((DataBag) line.get(0)).size();
		DataBag result = new DataBag();
		DataBag classBag=((DataBag) line.get(line.size()-1));
		
		if(sz==2){
			for (int i = 0; i < line.size()-2; i++) {
				for (int j = i+1; j < line.size()-1; j++) {
					DataBag d=new DataBag();
					d.add((Integer)((DataBag)line.get(i)).get(0));
					d.add((Integer)((DataBag)line.get(j)).get(0));
					d.add((Integer)((DataBag)line.get(i)).get(1));
					d.add((Integer)((DataBag)line.get(j)).get(1));
					result.add(d);
				}
				
			}
			result.add(classBag);
			//System.out.println(result);
			return result;
		}

		TreeMap<DataBag, DataBag> left = new TreeMap<DataBag, DataBag>();
		TreeMap<DataBag, DataBag> right = new TreeMap<DataBag, DataBag>();

		for (int i = 0; i < line.size() - 1; i++) {// last item is the label
													// item
			DataBag tmp = new DataBag((DataBag) line.get(i));
			tmp.remove(sz - 1);

			DataBag coreLeft = new DataBag(tmp);
			coreLeft.remove(coreLeft.size() - 1);
			DataBag coreRight = new DataBag(tmp);
			coreRight.remove(0);

			DataBag leftList = left.get(coreLeft);
			if (leftList == null)
				leftList = new DataBag();
			leftList.add(line.get(i));
			left.put(coreLeft, leftList);

			DataBag rightList = right.get(coreRight);
			if (rightList == null)
				rightList = new DataBag();
			rightList.add(line.get(i));
			right.put(coreRight, rightList);
		}
		// System.out.println("left "+left);
		// System.out.println("right "+right);

		left.remove(left.firstKey());
		right.remove(right.lastKey());

		for (Map.Entry<DataBag, DataBag> e : right.entrySet()) {

			DataBag start = e.getValue();

			DataBag stop = left.get(e.getKey());
			if (stop == null)
				continue;

			// System.out.println("start "+ start);
			// System.out.println("stop "+ stop);
			for (Object o1 : start) {
				DataBag is1 = (DataBag) o1;
				for (Object o2 : stop) {
					DataBag is2 = (DataBag) o2;
					DataBag compound = new DataBag(is1.size() + 2);
					for (int i = 0; i < is1.size() - 2; i++) {
						compound.add(is1.get(i));
					}
					compound.add(is2.get(is2.size() - 2));
					compound.add(is1.size() - 1);
					compound.add(is2.size() - 1);
					result.add(compound);
					// System.out.println("join "+ is1+"\t"+is2
					// +"=\t"+ compound);
				}
			}
		}
		result.add(classBag);
		return result;

	}

}
