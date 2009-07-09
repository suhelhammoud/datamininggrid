package others;

import java.util.*;

public class ValueMap extends TreeMap<Integer, Collection<Integer>> {

	private static final long serialVersionUID = 1L;

	// @Override
	final int compares(Object o1, Object o2) {
		Collection<Integer> v1 = get(o1);
		Collection<Integer> v2 = get(o2);
		int dif = v1.size() - v2.size();
		if (dif != 0)
			return dif;
		return ((Integer) o1).compareTo((Integer) o2);
	}

	@Override
	public Comparator<? super Integer> comparator() {
		// TODO Auto-generated method stub
		return super.comparator();
	}

	public static void main(String[] args) {
		ValueMap vm = new ValueMap();

		Integer[] i1 = new Integer[] { 1 };
		Set<Integer> s1 = new HashSet<Integer>(Arrays.asList(i1));

		Integer[] i2 = new Integer[] { 1, 2 };
		Set<Integer> s2 = new HashSet<Integer>(Arrays.asList(i2));

		Integer[] i3 = new Integer[] { 1, 2, 3 };
		Set<Integer> s3 = new HashSet<Integer>(Arrays.asList(i3));

		vm.put(1, s1);
		vm.put(2, s3);
		vm.put(3, s2);
		vm.put(4, s2);

		System.out.println(vm.toString());
	}

}