package others;

import java.math.BigInteger;
import java.util.ArrayList;
import java.util.Iterator;

public class ColumnId {
	final int length;
	final int cols;
	int[] index;
	BigInteger bi = BigInteger.ZERO;

	public ColumnId(int cols, int length) {
		this.length = length;
		this.cols = cols;
		this.index = new int[length];
	}

	/**
	 * @param args
	 */
	public static void main(String[] args) {
		// TODO Auto-generated method stub
		ArrayList<BigInteger> f;

	}

}
