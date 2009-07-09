package dm;

import org.apache.log4j.Logger;

import java.io.File;
import java.math.BigInteger;

import junit.framework.TestCase;

public class DataTest extends TestCase {
	/**
	 * Logger for this class
	 */
	private static final Logger logger = Logger.getLogger(DataTest.class);

	private Data data;

	public DataTest() {
		data = new Data();
		data.readFromFile("data/1.arff");
		logger.info("DataTest " + data.toString());
	}

	public void testToString() {
		String s = "Attributes: sepallength sepalwidth petallength petalwidth class \n"
				+ "data:\n"
				+ "1	1,1,1,1,1,\n"
				+ "2	2,2,1,1,2,\n"
				+ "3	1,1,3,1,2,\n" + "4	4,1,4,1,2,\n";
		// logger.info("\n"+data.toString());
		assertEquals(s.trim(), data.toString().trim());
	}

	public void testBitsLocations() {
		BigInteger b11 = new BigInteger("11");
		int[] a = { 0, 1, 3 };
		int[] result = Data.bitsLocations(b11);
		for (int i = 0; i < result.length; i++) {
			assertEquals(result[i], a[i]);
		}

		BigInteger b1 = new BigInteger("1");
		int[] a1 = { 0 };
		int[] result1 = Data.bitsLocations(b1);
		assertEquals(a1[0], result1[0]);

	}

	public void testOrgCol() {
		BigInteger b = new BigInteger("1");
		assertEquals(Data.orgCol(b), 0);

		b = new BigInteger("2");
		assertEquals(Data.orgCol(b), 1);

		b = new BigInteger("4");
		assertEquals(Data.orgCol(b), 2);

		b = new BigInteger("8");
		assertEquals(Data.orgCol(b), 3);

		b = new BigInteger("10");
		assertEquals(Data.orgCol(b), -1);

	}

}
