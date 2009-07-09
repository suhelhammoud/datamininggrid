package dm;

import org.apache.log4j.Logger;

import others.Tools;
import dm.*;
import java.math.*;
import java.util.Map;
import java.util.Set;

import junit.framework.TestCase;

public class DataMineTest extends TestCase {
	/**
	 * Logger for this class
	 */
	private static final Logger logger = Logger.getLogger(DataMineTest.class);

	protected void setUp() throws Exception {
		super.setUp();
	}

	public void testDataMine() {
		fail("Not yet implemented");
	}

	public void testGenerateAtomicColumnsOccurances() {
		fail("Not yet implemented");
	}

	public void testGenerateColumns() {
		logger.info("\n------------------- testGenerateColumns");
		Data data = new Data("data/1_lined.arff");
		DataMine datamine = new DataMine(data);

		Map<Long, Column> result = datamine.generateColumns(0, data.getLines());
		for (Column clmn : result.values()) {
			clmn.calcSupportsAndConfidences();
		}
		System.out.println("result ");
		System.out.println(result);
	}

	public void testBuildClassifier() {
		fail("Not yet implemented");
	}

	public void testPrintSupportsAndConfidences() {
		fail("Not yet implemented");
	}

	public static void main(String[] args) {
		logger.info("\n------------------- test occ");
		Data data = new Data("data/00.arff");
		System.out.println("data " + data);
		DataMine datamine = new DataMine(data);

		Map<Long, Column> result = datamine.generateColumns(0.00, data
				.getLines());
		System.out.println(Tools.join(result.entrySet(), "\n"));

	}

	private static void test() {
		logger.info("\n------------------- testGenerateColumns");
		Data data = new Data("data/1_lined.arff");
		Classifier classifier = new Classifier();

		DataMine datamine = new DataMine(data);
		Map<Long, Column> result = datamine.generateColumns(0.00, data
				.getLines());
		// System.out.println("support calcutlated \n"+Tools.join(result.entrySet(),"\n"));
		int sum = 0;
		for (Column clmn : result.values()) {
			sum += clmn.size();
		}
		for (Column clmn : result.values()) {
			classifier.addAllCandidateRules(clmn.generateForConfidence(0));
		}

		Set<Integer> trueCovered = classifier.build(data.getLines().size());
		// System.out.println("support calcutlated \n"+Tools.join(result.entrySet(),"\n"));
		System.out.println("size " + classifier.candidateRules.size() + "sum "
				+ sum);
		System.out.println("\n---------------------candidateRules ranked :\n"
				+ Tools.join(classifier.candidateRules, "\n"));
		/*
		 * System.out.println("\n-------------------rules\n "
		 * +Tools.join(classifier.rules, "\n"));
		 * System.out.println("\n-------trueCovered\n" +trueCovered);
		 * 
		 * System.out.println("\n------------- print as rule"); for (Sccl sccl :
		 * classifier.rules) { System.out.println("\n"+sccl.printAsRule(data));
		 * }
		 */
	}
}
