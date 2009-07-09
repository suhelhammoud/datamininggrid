package hdm;

import org.apache.log4j.Logger;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInput;
import java.io.DataInputStream;
import java.io.DataOutput;
import java.io.DataOutputStream;
import java.io.IOException;
import java.math.BigInteger;
import java.nio.ByteBuffer;
import java.util.Random;

import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.io.WritableComparable;

import com.sun.org.apache.bcel.internal.generic.LLOAD;

import dm.Sccl;

public class HRuleRank implements WritableComparable<Object> {
	/**
	 * Logger for this class
	 */
	private static final Logger logger = Logger.getLogger(HRuleRank.class);

	private static final int SIZEOF_RULE = (Double.SIZE + 4 * Integer.SIZE + Long.SIZE)
			/ Byte.SIZE;
	private static final int SIZEOF_DOUBLE = Double.SIZE / Byte.SIZE;
	private static final int SIZEOF_LONG = Long.SIZE / Byte.SIZE;
	private static final int SIZEOF_INT = Integer.SIZE / Byte.SIZE;

	public static final HRuleRank MAX_RANK() {
		return new HRuleRank(0, 0, Double.MAX_VALUE, Integer.MAX_VALUE,
				Integer.MAX_VALUE);
	}

	public static final HRuleRank MIN_RANK() {
		return new HRuleRank(Long.MAX_VALUE, Integer.MAX_VALUE, 0, 0, 0);
	}

	private long columnId;
	private int rowId;
	private double confidence;
	private int support;
	private int allOcc;

	public HRuleRank() {

	}

	public HRuleRank(long columnId, int rowId, double confidence, int support,
			int allOcc) {
		this.columnId = columnId;
		this.rowId = rowId;
		this.confidence = confidence;
		this.support = support;
		this.allOcc = allOcc;
	}

	public byte[] toBytes() {
		ByteBuffer bb = ByteBuffer.allocate(SIZEOF_RULE);
		bb.putDouble(-confidence);
		bb.putInt(-support);
		bb.putInt(+Long.bitCount(columnId));
		bb.putInt(-allOcc);
		bb.putLong(-columnId);
		bb.putInt(+rowId);
		return bb.array();
	}

	public static HRuleRank fromBytes(byte[] bytes) {
		ByteBuffer bb = ByteBuffer.wrap(bytes);
		double confidence = -bb.getDouble();
		int support = -bb.getInt();
		bb.position(SIZEOF_DOUBLE + SIZEOF_INT);
		int allOcc = -bb.getInt();
		long columnId = -bb.getLong();
		int rowId = +bb.getInt();
		return new HRuleRank(columnId, rowId, confidence, support, allOcc);
	}

	public byte[] toBytes2() throws IOException {

		byte[] result = new byte[SIZEOF_RULE];

		// double int int int long int
		byte[] bConf = Bytes.toBytes(confidence);// double
		byte[] bSupp = Bytes.toBytes(support);// int
		byte[] bBits = Bytes.toBytes(Long.bitCount(columnId));// int
		byte[] bOccs = Bytes.toBytes(allOcc);// int
		byte[] bCId = Bytes.toBytes(columnId);// long
		byte[] bRId = Bytes.toBytes(rowId);// int

		int start = 0;
		int length = bConf.length;
		System.arraycopy(bConf, 0, result, start, length);

		start += length;
		length = bSupp.length;
		System.arraycopy(bSupp, 0, result, start, length);

		start += length;
		length = bBits.length;
		System.arraycopy(bBits, 0, result, start, length);

		start += length;
		length = bOccs.length;
		System.arraycopy(bOccs, 0, result, start, length);

		start += length;
		length = bCId.length;
		System.arraycopy(bCId, 0, result, start, length);

		start += length;
		length = bRId.length;
		System.arraycopy(bRId, 0, result, start, length);

		return result;
	}

	public byte[] toBytes3() throws IOException {
		// Serialize to a byte array
		ByteArrayOutputStream bos = new ByteArrayOutputStream();
		DataOutput out = new DataOutputStream(bos);
		this.write(out);
		bos.close();
		// Get the bytes of the serialized object
		// byte[] buf = bos.toByteArray();
		return bos.toByteArray();
	}

	public static HRuleRank fromBytes3(byte[] bytes) throws IOException,
			ClassNotFoundException {
		DataInput in = new DataInputStream(new ByteArrayInputStream(bytes));
		HRuleRank result = new HRuleRank();
		result.readFields(in);
		return result;
	}

	@Override
	public void readFields(DataInput in) throws IOException {
		confidence = in.readDouble();
		support = in.readInt();
		in.readInt();
		allOcc = in.readInt();
		columnId = in.readLong();
		rowId = in.readInt();

	}

	@Override
	public void write(DataOutput out) throws IOException {
		out.writeDouble(confidence);
		out.writeInt(support);
		out.writeInt(Long.bitCount(columnId));
		out.writeInt(allOcc);
		out.writeLong(columnId);
		out.writeInt(rowId);

	}

	@Override
	public int compareTo(Object o) {
		HRuleRank s = (HRuleRank) o;
		// compare confidences;
		double doubleDif = confidence - s.confidence;
		if (Math.abs(doubleDif) > 0.000000001)
			return -(int) Math.signum(doubleDif);

		// compare support
		int dif = support - s.support;
		if (dif != 0)
			return -dif;

		// compare how many atomic column
		dif = Long.bitCount(columnId) - Long.bitCount(s.columnId);
		if (dif != 0)
			return dif;

		// compare allOcc
		dif = allOcc - s.allOcc;
		if (dif != 0)
			return -dif;

		// compare columns name
		dif = (int) (columnId - s.columnId);
		if (dif != 0)
			return dif;

		// compare rowIds
		dif = rowId - s.rowId;
		if (dif != 0)
			return -dif;

		logger.error("two equal ranked rules");

		return 0;
	}

	public static void main(String[] args) throws IOException {

		Random rnd = new Random();
		System.out.println(Long.bitCount(1));
		for (long i = 0; i < 1000; i++) {
			long l = Math.abs(rnd.nextLong());

			BigInteger bi = BigInteger.valueOf(l);
			if (bi.bitCount() != Long.bitCount(l)) {
				logger.error("not equal on " + l);
				System.out.println(bi + "\t" + bi.bitCount());
				System.out.println(l + "\t" + Long.bitCount(l));
				break;
			}

		}
		// testToBytes();
		// testCompare();
		System.out.println("---------------------");
		// testScclToarry();
	}

	private static void testToBytes() throws IOException {

		int count = 1000;
		long t;

		Random rnd = new Random();
		long sum1 = 0, sum2 = 0, sum3 = 0;

		for (int n = 0; n < 1000; n++) {

			HRuleRank rr = new HRuleRank(rnd.nextLong(), rnd.nextInt(), rnd
					.nextDouble(), rnd.nextInt(), rnd.nextInt());

			t = System.nanoTime();
			for (int i = 0; i < count; i++) {
				byte[] bytes = rr.toBytes();
			}
			sum1 += System.nanoTime() - t;

			t = System.nanoTime();
			for (int j = 0; j < count; j++) {
				byte[] bytes = rr.toBytes2();
			}
			sum2 += System.nanoTime() - t;

			t = System.nanoTime();
			for (int i = 0; i < count; i++) {
				byte[] bytes = rr.toBytes3();
			}
			sum3 += System.nanoTime() - t;
		}
		System.out.println("sum 1 " + sum1);
		System.out.println("sum 2 " + sum2);
		System.out.println("sum 3 " + sum3);
		print(sum1, sum1, sum2, sum3);
		print(sum2, sum1, sum2, sum3);
		print(sum3, sum1, sum2, sum3);

	}

	public static void testScclToarry() throws IOException {
		long sum1, sum2, sum3;
		sum1 = 0;
		sum2 = 0;
		sum3 = 0;
		Random rnd = new Random();
		int count = 1000;

		long t;
		for (int i = 0; i < 100; i++) {

			Sccl h = new Sccl(rnd.nextLong());
			for (int line = 0; line < 1000; line++) {
				h.addLine(line, rnd.nextInt(100));
			}

			byte[] b1 = null;
			byte[] b2 = null;

			t = System.nanoTime();
			for (int j = 0; j < count; j++) {
				h.toBytes();
			}
			sum2 += System.nanoTime() - t;

			t = System.nanoTime();
			for (int j = 0; j < count; j++) {
				h.toBytes_old();
			}
			sum1 += System.nanoTime() - t;

			// if (compareBytes(b1, b2) != 0) {
			// System.out.println("not equalts");
			// return;
			// }
			//	
			//	
		}
		System.out.println("test sccl");
		System.out.println("sum 1 " + sum1);
		System.out.println("sum 2 " + sum2);
		print(sum1, sum1, sum2, sum3);
		print(sum2, sum1, sum2, sum3);

	}

	static void print(long result, long sum1, long sum2, long sum3) {
		System.out.println("\n" + (double) sum1 / result + "\t" + (double) sum2
				/ result + "\t" + (double) sum3 / result);

	}

	public static void testScclCompare() throws IOException {
		long sum1, sum2, sum3;
		sum1 = 0;
		sum2 = 0;
		sum3 = 0;
		Random rnd = new Random();
		int count = 10000;

		long t;
		for (int i = 0; i < count; i++) {

			long cid = rnd.nextLong();

			Sccl h3 = new Sccl(cid);
			Sccl h4 = new Sccl(cid);
			Sccl h1 = new Sccl(cid);
			Sccl h2 = new Sccl(cid);

			for (int line = 0; line < 1000; line++) {
				Integer lbl = rnd.nextInt(50);

				h3.addLine(line, lbl);
				h4.addLine(line, lbl);
				h1.addLine(line, lbl);
				h2.addLine(line, lbl);
			}

			t = System.nanoTime();
			h1.compareTo(h2);
			sum1 += System.nanoTime() - t;

			t = System.nanoTime();
			byte[] r1 = h3.hRuleRank().toBytes();
			byte[] r2 = h4.hRuleRank().toBytes();
			compareBytes(r1, r2);
			sum2 += System.nanoTime() - t;

		}
		System.out.println("test sccl");
		print(sum1, sum1, sum2, sum3);
		print(sum2, sum1, sum2, sum3);
	}

	public static void testCompare() throws IOException {
		long sum1, sum2, sum3;
		sum1 = 0;
		sum2 = 0;
		sum3 = 0;
		Random rnd = new Random();
		int count = 1000;

		long t;
		for (int i = 0; i < 10; i++) {
			HRuleRank rr1 = new HRuleRank(rnd.nextLong(), rnd.nextInt(), rnd
					.nextDouble(), rnd.nextInt(), rnd.nextInt());
			byte[] b1 = rr1.toBytes();
			byte[] b2 = rr1.toBytes2();

			t = System.nanoTime();
			for (int j = 0; j < count; j++) {
				Bytes.compareTo(b1, b2);
			}
			sum1 += System.nanoTime() - t;

			t = System.nanoTime();
			for (int j = 0; j < count; j++) {
				compareBytes(b1, b2);
			}
			sum2 += System.nanoTime() - t;

		}
		System.out.println("compare");
		print(sum1, sum1, sum2, sum3);
		print(sum2, sum1, sum2, sum3);
	}

	public static int compareBytes(byte[] b1, byte[] b2) {

		for (int i = 0, j = 0; i < b1.length && j < b2.length; i++, j++) {
			int a = (b1[i] & 0xff);
			int b = (b2[j] & 0xff);
			if (a != b) {
				return a - b;
			}
		}
		return b1.length - b2.length;
	}

}
