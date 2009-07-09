package dm;

import org.apache.log4j.Logger;

import others.Tools;

import java.math.BigInteger;
import java.util.*;
import java.util.Map.Entry;

public class Classifier {
	/**
	 * Logger for this class
	 */
	private static final Logger logger = Logger.getLogger(Classifier.class);

	TreeSet<Sccl> candidateRules = new TreeSet<Sccl>();

	Map<Sccl, ScclRule> ruleMap = new LinkedHashMap<Sccl, ScclRule>();


	public boolean addCandidateRule(Sccl sccl) {
		return candidateRules.add(sccl);
	}

	public boolean addAllCandidateRules(List<Sccl> set) {
		return candidateRules.addAll(set);
	}

	// public boolean addRule(Sccl sccl) {
	// return rules.add(sccl);
	// }



	public void buildMClassifier(Set<Integer> allLines, int iteration) {
		CheckResult result = buildMulti(allLines);

		int count = 1;
		while (result.trueCovered.size() > 0 && count < iteration) {
			allLines.removeAll(result.trueCovered);
			result = buildMulti(allLines);
			logger.info("iterataion " + (++count));
			if (result.trueCovered.size() == 0)
				break;
		}

	}


	public Set<Integer> buildMapReduce(Data data){
		//Data data = new Data(filename);
		ScclRule.numOfAttributes = data.getNumberOfColumns();
		ruleMap.clear();
		//DataMine datamine = new DataMine(data);

		//		Map<Long, Column> columnsMap = datamine.generateColumns(support, data
		//				.getLines());
		//		
		Map<Integer, Sccl> lineMap=new HashMap<Integer, Sccl>();
		for (Sccl sccl : candidateRules) {
			for (Integer i : sccl.getAllLines()) {
				Sccl other=lineMap.get(i);
				if(other==null)lineMap.put(i, sccl);
				else{
					if(sccl.compareTo(other)<1)
						lineMap.put(i, sccl);
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

		//Map<Sccl, ScclRule> ruleMap = new TreeMap<Sccl, ScclRule>();

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

//		for (ScclRule i : ruleMap.values()) {
//			i.fill(data);
//			System.out.println(i);
//		}
		return lineMap.keySet();

	}
	public CheckResult buildMulti(Set<Integer> allLines) {
		CheckResult result = new CheckResult(0, new TreeSet<Integer>(),
				new TreeSet<Integer>(), new TreeSet<Integer>());
		// TODO: change the TreeSet to HashSet for the performance
		Set<Integer> taggedLines = new TreeSet<Integer>();
		// Set<Integer> notCovered=new TreeSet<Integer>();

		CheckResult checkResult;
		for (Sccl sccl : candidateRules) {
			checkResult = sccl.check(taggedLines, allLines);

			if (checkResult.trueCovered.size() == 0)
				continue;

			// check if rule is already in the ruleMap
			ScclRule scclRule;
			if (ruleMap.containsKey(sccl)) {
				scclRule = ruleMap.get(sccl);
				if (scclRule == null) {
					logger.error("ruleMap.get(sccl=" + sccl.columnId + ","
							+ sccl.getRowId() + ")=null");
					scclRule = new ScclRule(sccl);
					ruleMap.put(sccl, scclRule);
				}
			} else {
				scclRule = new ScclRule(sccl);
				ruleMap.put(sccl, scclRule);
			}
			scclRule.addLabelFreq(checkResult.trueCovered.size(),
					checkResult.label);

			taggedLines.addAll(checkResult.trueCovered);
			taggedLines.addAll(checkResult.falseCoverd);
			// notCovered.addAll(checkResult.falseCoverd);

			result.trueCovered.addAll(checkResult.trueCovered);
			result.falseCoverd.addAll(checkResult.falseCoverd);

			if (taggedLines.size() == allLines.size())
				break;
		}
		return result;
	}

	public Set<Integer> build(int allSize) {
		// TODO: change the TreeSet to HashSet for the performance
		Set<Integer> taggedLines = new TreeSet<Integer>();
		Set<Integer> result = new TreeSet<Integer>();

		CheckResult checkResult;
		for (Sccl sccl : candidateRules) {
			checkResult = sccl.check(taggedLines);

			if (checkResult.trueCovered.size() == 0)
				continue;

			// check if rule is already in the ruleMap
			ScclRule scclRule;
			if (ruleMap.containsKey(sccl)) {
				scclRule = ruleMap.get(sccl);
				if (scclRule == null) {
					logger.error("ruleMap.get(sccl=" + sccl.columnId + ","
							+ sccl.getRowId() + ")=null");
					scclRule = new ScclRule(sccl);
					ruleMap.put(sccl, scclRule);
				}
			} else {
				scclRule = new ScclRule(sccl);
				ruleMap.put(sccl, scclRule);
			}
			scclRule.addLabelFreq(checkResult.trueCovered.size(),
					checkResult.label);

			taggedLines.addAll(checkResult.trueCovered);
			taggedLines.addAll(checkResult.falseCoverd);
			// notCovered.addAll(checkResult.falseCoverd);
			result.addAll(checkResult.trueCovered);
			if (taggedLines.size() == allSize)
				break;
		}
		return result;
	}

	public Sccl buildDefultSccl(Set<Integer> wlines, Data data) {
		Sccl sccl = new Sccl(0, 0);
		for (Integer line : wlines) {
			sccl.addLine(line, data.get(line));
		}
		return sccl;
	}

	void fillScclRules(Data data) {
		ScclRule.numOfAttributes = data.getNumberOfColumns();
		for (ScclRule scclRule : ruleMap.values()) {
			scclRule.fill(data);
		}
	}

	public Integer predictOne(List<Integer> line) {
		for (ScclRule scclRule : ruleMap.values()) {
			if (scclRule.accept(line))
				return scclRule.getOne().intLabel;
		}
		return -1;
	}

	public Integer predictOne(int[] line) {
		for (ScclRule scclRule : ruleMap.values()) {
			if (scclRule.accept(line))
				return scclRule.getOne().intLabel;
		}
		return -1;
	}

	@Override
	public String toString() {

		return Tools.join(ruleMap.keySet(), "\n");
	}

	public static void main(String[] args) {
		// TODO Auto-generated method stub
		Data data = new Data("data/1_lined.arff");
		System.out.println(data);
		Classifier classifier = new Classifier();

		System.out.println(classifier.buildDefultSccl(data.getLines(), data));
	}
}
