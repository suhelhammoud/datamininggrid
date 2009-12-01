package utils;

import java.util.ArrayList;
import java.util.List;

public class SubSets {



	static void subset(int num){

	}

	public static void main(String[] args) {
		//test1();
		subsets((byte)13);
	}

	public static void test1() {
		List<Integer> foo = new ArrayList<Integer>();
		for (int i = 0; i < 3; i++) foo.add(i);
		int[] bar = superSet(foo);
		String sep = "";
		System.out.print("[");
		for (int set : bar) {
			System.out.print(sep + "[");
			sep = "";
			for (int i = 0; i < foo.size(); i++) {
				if ((set & (1 << i)) > 0) {
					System.out.print(sep + foo.get(i));
					sep = ", ";
				}
			}
			System.out.print("]");
			sep = ", ";
		}
		System.out.println("]");
	}
	public static int[] superSet(List<Integer> values) {
		int superSetCount = (int)Math.pow(2, values.size());
		int[] result = new int[superSetCount];
		for (int i = 0; i < superSetCount; i++) {
			result[i] = i;
		}
		return result;
	}
	static void subsets(byte N) {
		long n = 1<<N; // 2^N
		System.out.println("range="+n);
		for (long s=0; s<n; s++) { // all subsets
			System.out.print(s + ": ");
			for (long i=0; i<N; i++) { // check bits in subset
				if ((s & (1<<i)) != 0) // bit is set
					;
					//System.out.print(i + " ");
			}
			System.out.println();
		}
		System.out.println("done");
	}		

}
