package dm;

import org.apache.log4j.Logger;

import filters.IMap;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;
import java.util.TreeSet;

import others.Tools;

public class MapReduceClassifier {
	/**
	 * Logger for this class
	 */
	private static final Logger logger = Logger.getLogger(MapReduceClassifier.class);

	public static void mapreduceBuild(String filename, int support,double confidence){
		Data data = new Data(filename);
		ScclRule.numOfAttributes = data.getNumberOfColumns();
		DataMine datamine = new DataMine(data);

		Map<Long, Column> columnsMap = datamine.generateColumns(support, data
				.getLines());
		
		Map<Integer, Sccl> lineMap=new HashMap<Integer, Sccl>();
		for (Column clmn : columnsMap.values()) {
			//List<Sccl> list=clmn.generateForConfidence(confidence);
			List<Sccl> list=new ArrayList<Sccl>(clmn.getItems().values());
			for (Sccl sccl : list) {
				for (Integer i : sccl.getAllLines()) {
					Sccl other=lineMap.get(i);
					if(other==null)lineMap.put(i, sccl);
					else{
						if(sccl.compareTo(other)<1)
							lineMap.put(i, sccl);
					}
				}
			}
		}
		
		Map<Sccl,List<Integer>> ruleCount=new TreeMap<Sccl, List<Integer>>();
		
		for (Map.Entry<Integer, Sccl> iter : lineMap.entrySet()) {
			Sccl sccl=iter.getValue();
			List<Integer> list=ruleCount.get(sccl);
			if (list==null) {
				list=new ArrayList<Integer>();
			}
			list.add(data.get(iter.getKey()));
			ruleCount.put(sccl, list);
		}
		
		Map<Sccl, ScclRule> ruleMap = new TreeMap<Sccl, ScclRule>();
		
		for (Map.Entry<Sccl,List<Integer>> iter : ruleCount.entrySet()) {
			List<Integer> list=iter.getValue();
			Map<Integer, Integer> imap=new HashMap<Integer, Integer>();
			for (Integer i : list)imap.put(i, imap.get(i)==null?1:1+imap.get(i));
			
			Sccl sccl=iter.getKey();
			ScclRule scclRule=sccl.getScclRule();
			for (Integer i : imap.keySet())scclRule.addLabelFreq(imap.get(i), i);
			scclRule.fill(data);
			ruleMap.put(sccl, scclRule);
		}

		for (ScclRule i : ruleMap.values()) {
			i.fill(data);
			System.out.println(i);
		}

	}
	public static void main(String[] args) {
		mapreduceBuild("data/in/arff/00.arff", 3, 0.4);
	}
}
