package dfs;


import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

public class ReplicaMap {

	public static Map<HSplit, Set<String>> map = new LinkedHashMap<HSplit, Set<String>>();
	private static long idIndex = 0;

	public static long getId() {
		return (++idIndex);
	}

	public static boolean replicate(HSplit split, String dataNode) {
		Set<String> set = map.get(split);
		if (set == null)
			set = new LinkedHashSet<String>();
		map.put(split, set);
		return set.add(dataNode);
	}

	public static String print() {
		StringBuffer sb = new StringBuffer("ReplicaMap\n");
		for (HSplit split : map.keySet()) {
			sb.append("split:" + split + "\t nodes:");
			Set<String> set = map.get(split);
			for (String node : set) {
				sb.append("\t" + node);
			}
			sb.append("\n");
		}
		return sb.toString();
	}

}
