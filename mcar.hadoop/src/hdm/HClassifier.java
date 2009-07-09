package hdm;

import java.io.IOException;
import java.math.BigInteger;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeSet;

import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.io.BatchUpdate;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.log4j.Logger;

import others.Tools;

import dm.*;

public class HClassifier extends Classifier {
	/**
	 * Logger for this class
	 */
	private static final Logger logger = Logger.getLogger(HClassifier.class);

	HBaseConfiguration conf;
	HTable hcandidateRules;

	public HClassifier(HBaseConfiguration conf) {
		this.conf = conf;
		try {
			hCandidateRules = new HTable(conf, conf.get("classifier",
					"classifier"));
		} catch (IOException e) {
			e.printStackTrace();
		}
	}

	public void createCandidateTable() throws IOException {
		hcandidateRules = HUtil.create(conf, conf.get("hcandidateRules",
				"hcandidateRules").getBytes());
	}

	public boolean addHCandidateRule(Sccl sccl) {
		HRuleRank rr = sccl.hRuleRank();
		BatchUpdate batchUpdate = new BatchUpdate(rr.toBytes());
		batchUpdate.put(HUtil.FAMILY, sccl.toBytes());
		return true;
	}

	// ///////////////////////////////
	// TODO H
	HTable hCandidateRules = null;// new HTable("name");
	List<Sccl> rules = new ArrayList<Sccl>();
	// List<ScclRule> scclRules=new ArrayList<ScclRule>();

	Map<Sccl, ScclRule> ruleMap = new LinkedHashMap<Sccl, ScclRule>();

	// List<String[]> rules=new ArrayList<String[]>();
	// Map<Integer, String> pridectedClassMap=new TreeMap<Integer, String>();
	// Map<BigInteger, Sccl> rules=new LinkedHashMap<BigInteger, Sccl>();

	public boolean addCandidateRule(Sccl sccl) {
		// return candidateRules.add(sccl);
		return false;
	}

	public boolean addAllCandidateRules(List<Sccl> set) {
		// return candidateRules.addAll(set);
		return false;
	}

	public boolean addRule(Sccl sccl) {
		return rules.add(sccl);
	}

	public ScclRule predictLine(List<String> line) {
		for (ScclRule scclRule : ruleMap.values()) {

		}
		return null;
	}

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

	public CheckResult buildMulti(Set<Integer> allLines) {
		CheckResult result = new CheckResult(0, new TreeSet<Integer>(),
				new TreeSet<Integer>(), new TreeSet<Integer>());
		// TODO: change the TreeSet to HashSet for the performance
		Set<Integer> taggedLines = new TreeSet<Integer>();
		// Set<Integer> notCovered=new TreeSet<Integer>();

		CheckResult checkResult;
		for (Sccl sccl : hCandidateRules()) {
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

	private List<Sccl> hCandidateRules() {
		// TODO Auto-generated method stub
		return null;
	}

	public Set<Integer> build(int allSize) {
		// TODO: change the TreeSet to HashSet for the performance
		Set<Integer> taggedLines = new TreeSet<Integer>();
		Set<Integer> result = new TreeSet<Integer>();

		CheckResult checkResult;
		for (Sccl sccl : hCandidateRules()) {
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
				return scclRule.getOne().intLabel();
		}
		return -1;
	}

	public Integer predictOne(int[] line) {
		for (ScclRule scclRule : ruleMap.values()) {
			if (scclRule.accept(line))
				return scclRule.getOne().intLabel();
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
