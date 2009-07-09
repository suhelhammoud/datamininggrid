package hdm;

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
import java.util.TreeSet;

import org.apache.hadoop.io.WritableComparable;

import dm.Column;
import dm.Data;
import dm.Sccl;

public class Test extends TreeSet<Integer> implements
		WritableComparable<Object> {

	@Override
	public void readFields(DataInput in) throws IOException {
		// TODO Auto-generated method stub
		clear();
		int sz = in.readInt();
		for (int i = 0; i < sz; i++) {
			add(in.readInt());
		}
	}

	@Override
	public void write(DataOutput out) throws IOException {
		// TODO Auto-generated method stub
		out.writeInt(size());
		for (Integer item : this) {
			out.writeInt(item);

		}
	}

	@Override
	public int compareTo(Object o) {
		// TODO Auto-generated method stub
		return 0;
	}

	byte[] toArr2() throws IOException {
		ByteArrayOutputStream bos = new ByteArrayOutputStream();
		DataOutput out = new DataOutputStream(bos);
		this.write(out);
		bos.close();
		// Get the bytes of the serialized object
		// byte[] buf = bos.toByteArray();
		return bos.toByteArray();
	}

	byte[] toArr() throws IOException {
		ByteBuffer bb = ByteBuffer.allocate((size() + 1) * Integer.SIZE
				/ Byte.SIZE);
		bb.putInt(size());
		for (Integer item : this) {
			bb.putInt(item);
		}
		return bb.array();
	}

	void fromArr2(byte[] b) throws IOException {
		DataInput in = new DataInputStream(new ByteArrayInputStream(b));
		readFields(in);
	}

	void fromArr(byte[] b) {
		clear();
		ByteBuffer bb = ByteBuffer.wrap(b);
		int sz = bb.getInt();
		for (int i = 0; i < sz; i++) {
			add(bb.getInt());
		}
	}

	public static void main(String[] args) throws IOException {
		// testToArr();
		// System.out.println(BigInteger.TEN.bitCount());
		// System.out.println(BigInteger.TEN.bitLength());
		//		
		// testBitslocatins();

	}

	private static void testBitslocatins() {
		Random rnd = new Random();
		long sum1 = 0, sum2 = 0, t = 0;
		int count = 100000;

		for (int i = 0; i < 10; i++) {
			long l = Math.abs(rnd.nextLong());
			BigInteger bi = BigInteger.valueOf(l);

			int[] loc1 = null;
			int[] loc2 = null;

			t = System.nanoTime();
			for (int j = 0; j < count; j++) {
				loc2 = Data.bitsLocations(bi);
			}
			sum2 += System.nanoTime() - t;

			t = System.nanoTime();
			for (int j = 0; j < count; j++) {
				loc1 = Data.bitsLocations(l);
			}
			sum1 += System.nanoTime() - t;

			if (compareInts(loc1, loc2) != 0) {
				System.out.println("error at " + l);
				break;
			}

		}
		System.out.println("done");
		System.out.println("sum1 =" + sum1);
		System.out.println("sum2 =" + sum2);
		System.out.println("sum2/sum1 =" + (double) sum2 / sum1);
	}

	public static int compareInts(int[] b1, int[] b2) {

		for (int i = 0, j = 0; i < b1.length && j < b2.length; i++, j++) {
			int a = b1[i];
			int b = b2[j];
			if (a != b) {
				return a - b;
			}
		}
		return b1.length - b2.length;
	}

	public static int bitCount(long i) {
		// HD, Figure 5-14
		i = i - ((i >>> 1) & 0x5555555555555555L);
		i = (i & 0x3333333333333333L) + ((i >>> 2) & 0x3333333333333333L);
		i = (i + (i >>> 4)) & 0x0f0f0f0f0f0f0f0fL;
		i = i + (i >>> 8);
		i = i + (i >>> 16);
		i = i + (i >>> 32);
		return (int) i & 0x7f;
	}

	public static void testToArr() throws IOException {
		long sum1 = 0, sum2 = 0;

		Test s = new Test();
		Random rnd = new Random(System.nanoTime());

		int count = 100;
		long t = 0;

		for (int i = 0; i < 10000; i++) {

			s.clear();
			for (int j = 0; j < 10; j++) {
				s.add(rnd.nextInt());
			}

			byte[] b = null;
			t = System.nanoTime();
			for (int j = 0; j < count; j++) {
				b = s.toArr();
			}
			sum1 += System.nanoTime() - t;

			t = System.nanoTime();
			for (int j = 0; j < count; j++) {
				s.toArr2();
			}
			sum2 += System.nanoTime() - t;

		}

		System.out.println("sum1 =" + sum1);
		System.out.println("sum2 =" + sum2);
		System.out.println("/ =" + (double) sum1 / sum2);
	}

	public static void testFromArr() throws IOException {
		long sum1 = 0, sum2 = 0;

		Test s = new Test();
		Random rnd = new Random(System.nanoTime());

		int count = 100;
		long t = 0;

		for (int i = 0; i < 1000; i++) {

			s.clear();
			for (int j = 0; j < 1000; j++) {
				s.add(rnd.nextInt());
			}

			byte[] b = s.toArr();

			t = System.nanoTime();
			for (int j = 0; j < count; j++) {
				s.fromArr2(b);
			}
			sum2 += System.nanoTime() - t;

			t = System.nanoTime();
			for (int j = 0; j < count; j++) {
				s.fromArr(b);
			}
			sum1 += System.nanoTime() - t;

		}

		System.out.println("sum1 =" + sum1);
		System.out.println("sum2 =" + sum2);
		System.out.println("/ =" + (double) sum1 / sum2);
	}

}
