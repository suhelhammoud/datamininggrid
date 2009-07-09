package dm;

import org.apache.log4j.Logger;

import java.math.BigInteger;

import junit.framework.TestCase;

public class ScclTest extends TestCase {
	/**
	 * Logger for this class
	 */
	private static final Logger logger = Logger.getLogger(ScclTest.class);

	private Sccl sccl;

	protected void setUp() throws Exception {
		sccl = new Sccl(1L);
	}

	public void testHashCode() {
		fail("Not yet implemented");
	}

	public void testScclBigInteger() {
		fail("Not yet implemented");
	}

	public void testScclBigIntegerInteger() {
		fail("Not yet implemented");
	}

	public void testScclBigIntegerIntIntegerTreeSetOfInteger() {
		fail("Not yet implemented");
	}

	public void testScclSccl() {
		fail("Not yet implemented");
	}

	public void testScclScclSccl() {
		fail("Not yet implemented");
	}

	public void testMinimumLine() {
		fail("Not yet implemented");
	}

	public void testScclConstructor() {
		logger.info("\n----------------------------testScclConstructor");
		Sccl s1 = new Sccl(1);
		Sccl s2 = new Sccl(2);
		Sccl sNull = new Sccl(4);

		s1.addLine(2, 1);
		s1.addLine(3, 2);
		s1.addLine(4, 2);
		s1.addLine(5, 2);

		s2.addLine(1, 1);
		s2.addLine(2, 1);
		s2.addLine(3, 2);
		s2.addLine(4, 2);

		logger.info("s1=" + s1);
		logger.info("s2 " + s2);

		Sccl s3 = Sccl.compose(3L, s1, s2);
		logger.info("s3 " + s3);

		Sccl s5 = Sccl.compose(5L, s1, sNull);
		logger.info("s5 " + s5);

	}

	public void testAddLine() {
		logger.info("\n----------------------------testAddLine");
		sccl.addLine(1, 1);
		sccl.addLine(2, 1);
		sccl.addLine(3, 2);
		sccl.addLine(4, 1);
		sccl.addLine(5, 2);

		sccl.getSupport();
		sccl.getConfidence();
		logger.info(sccl);

		sccl.addLine(6, 3);
		sccl.addLine(7, 1);
		logger.info(sccl);

		assertEquals(5, sccl.getAllLines().size());
	}

	public void testCompareTo() {
		fail("Not yet implemented");
	}

	public void testEqualsObject() {
		fail("Not yet implemented");
	}

	public void testGetLines() {
		fail("Not yet implemented");
	}

	public void testGetAllLines() {
		fail("Not yet implemented");
	}

	public void testGetColumnId() {
		fail("Not yet implemented");
	}

	public void testGetConfidence() {
		fail("Not yet implemented");
	}

	public void testMainItem() {
		fail("Not yet implemented");
	}

	public void testGet() {
		fail("Not yet implemented");
	}

	public void testGetRowId() {
		fail("Not yet implemented");
	}

	public void testGetSupport() {
		fail("Not yet implemented");
	}

	public void testGetKey() {
		fail("Not yet implemented");
	}

	public void testIsCovering() {
		fail("Not yet implemented");
	}

	public void testToString() {
		fail("Not yet implemented");
	}

	public static void main(String[] args) {
		ScclTest test = new ScclTest();

		test.testScclConstructor();
	}
}
