package dm;

import org.apache.log4j.Logger;

import com.sun.jndi.toolkit.ctx.Continuation;

import java.math.BigInteger;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;

import junit.framework.TestCase;

public class ColumnTest extends TestCase {
	/**
	 * Logger for this class
	 */
	private static final Logger logger = Logger.getLogger(ColumnTest.class);

	private Data initData(String dataFile) {
		Data data = new Data();
		data.readFromFile(dataFile);
		return data;
	}

	public void testProduct() {
		int[] a1 = { 1, 2, 3, 4, 5 };
		int[] a2 = { 11, 22, 33, 44 };
		Set set1 = new HashSet<Integer>(a1.length);
		Set set2 = new HashSet<Integer>(a2.length);
		for (int i = 0; i < a1.length; i++) {
			set1.add(a1[i]);
		}
		for (int i = 0; i < a2.length; i++) {
			set2.add(a2[i]);
		}
		Collection<int[]> result = Column.product(set1, set2);
		assertEquals(result.size(), a1.length * a2.length);
		for (int[] is : result) {
			assertTrue(set1.contains(is[0]) && set2.contains(is[1]));
		}
	}

	public void testFirstSubColumn() {
		BigInteger b11 = new BigInteger("11");
		BigInteger b10 = new BigInteger("10");
		assertEquals(Column.firstSubColumn(b11), b10);
	}

	public void testSecondSubColumn() {
		BigInteger b11 = new BigInteger("11");
		BigInteger b03 = new BigInteger("03");
		assertEquals(Column.secondSubColumn(b11), b03);
	}

	public void testGenerateAtomicOccurances() {
		logger.info("\n------------------- testGenerateOccurances");
		Data data = initData("data/1_lined.arff");

		logger.info("data=" + data);
		Set<Integer> allLines = data.getLines();
		assertEquals(allLines.size(), 4);

		long maxColumnID = DataMine.maxColumnID(6);

		// assertEquals(maxColumnID, 15);
		long id = 1;

		for (int i = 1; i <= maxColumnID; i *= 2) {
			id = i;
			Column ci = new Column(id);
			ci.generateAtomicOccurances(allLines, data);
			ci.calcSupportsAndConfidences();
			logger.info(" ci: " + i + " " + ci.toString());

		}
	}

	public void testGenerateAtomicForSupport() {
		logger.info("\n------------------- testGenerateAtomicForSupport");

		Data data = initData("data/1_lined.arff");
		Set<Integer> allLines = data.getLines();
		long maxColumnID = DataMine.maxColumnID(6);// with the class column
		long id = 1;

		for (int i = 1; i <= maxColumnID; i *= 2) {
			id = i;
			Column ci = new Column(id);
			ci.generateAtomicOccurances(allLines, data);
			ci.generateAtomicForSupport(1, allLines, data);
			logger.info(" ci: " + i + " " + ci.toString());

		}

	}

	public void testGenerateForConfidence() {
		logger.info("\n------------------- testGenerateForConfidence");

		Data data = initData("data/1_lined.arff");
		Set<Integer> allLines = data.getLines();
		long maxColumnID = DataMine.maxColumnID(6);// with the class column
		long id = 1;

		for (long i = 1; i <= maxColumnID; i *= 2) {
			id = i;
			Column ci = new Column(id);
			ci.generateAtomicOccurances(allLines, data);
			ci.generateAtomicForSupport(1, allLines, data);
			ci.generateForConfidence(0.00);
			logger.info(" ci: " + i + " " + ci.toString());

		}
	}

	public void testGenerateForSupport() {
		logger.info("\n------------------- testGenerateForSupport");

		Data data = initData("data/1_lined.arff");
		Set<Integer> allLines = data.getLines();

		// generate atomic

		Column c1 = new Column(1);
		Column c2 = new Column(2);
		Column c3 = new Column(3);

		c1.generateAtomicForSupport(0, allLines, data);
		c2.generateAtomicForSupport(0, allLines, data);

		logger.info("atomic " + c1);
		logger.info("atomic " + c2);
		logger.info("complex before" + c3);

		c3.generateForSupport(0, c1, c2);

		c1.calcSupportsAndConfidences();

		logger.info("complex after" + c3);

	}

	public static void main(String[] args) {
		ColumnTest test = new ColumnTest();

		logger.info("data" + test.initData("data/1_lined.arff"));

		test.testGenerateForSupport();
	}
}
