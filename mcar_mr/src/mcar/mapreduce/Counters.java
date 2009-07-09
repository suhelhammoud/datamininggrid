package mcar.mapreduce;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.IdentityHashMap;
import java.util.Iterator;
import java.util.Map;

public class Counters implements  Iterable<Counters.Group> {
	


	/**
	 * A counter record, comprising its name and value. 
	 */
	public static class Counter  {

		private String name;
		private String displayName;
		private long value;

		Counter() { 
			value = 0L;
		}

		Counter(String name, String displayName, long value) {
			this.name = name;
			this.displayName = displayName;
			this.value = value;
		}

		

		/**
		 * @return the internal name of the counter
		 */
		public synchronized String getName() {
			return name;
		}

		/**
		 * @return the user facing name of the counter
		 */
		public synchronized String getDisplayName() {
			return displayName;
		}

		/**
		 * Set the display name of the counter.
		 */
		public synchronized void setDisplayName(String displayName) {
			this.displayName = displayName;
		}


		// Checks for (content) equality of two (basic) counters
		synchronized boolean contentEquals(Counter c) {
			return name.equals(c.getName())
			&& displayName.equals(c.getDisplayName())
			&& value == c.getCounter();
		}

		/**
		 * @return the current value
		 */
		public synchronized long getCounter() {
			return value;
		}

		/**
		 * @param incr the value to increase this counter by
		 */
		public synchronized void increment(long incr) {
			value += incr;
		}
	}

	public static class Group implements  Iterable<Counter> {
		private String groupName;
		private String displayName;
		private Map<String, Counter> subcounters = new HashMap<String, Counter>();

		// Optional ResourceBundle for localization of group and counter names.
		//private ResourceBundle bundle = null;    

		public Group(String groupName) {
//			try {
//				bundle = getResourceBundle(groupName);
//			}
//			catch (MissingResourceException neverMind) {
//			}
			this.groupName = groupName;
			this.displayName = groupName;
		}

		

		public String getName() {
			return groupName;
		}

		public String getDisplayName() {
			return displayName;
		}

		public void setDisplayName(String displayName) {
			this.displayName = displayName;
		}



		synchronized boolean contentEquals(Group g) {
			boolean isEqual = false;
			if (g != null) {
				if (size() == g.size()) {
					isEqual = true;
					for (Map.Entry<String, Counter> entry : subcounters.entrySet()) {
						String key = entry.getKey();
						Counter c1 = entry.getValue();
						Counter c2 = g.getCounterForName(key);
						if (!c1.contentEquals(c2)) {
							isEqual = false;
							break;
						}
					}
				}
			}
			return isEqual;
		}

		public synchronized long getCounter(String counterName) {
			for(Counter counter: subcounters.values()) {
				if (counter != null && counter.displayName.equals(counterName)) {
					return counter.value;
				}
			}
			return 0L;
		}

		@Deprecated
		public synchronized Counter getCounter(int id, String name) {
			return getCounterForName(name);
		}

		public synchronized Counter getCounterForName(String name) {
			Counter result = subcounters.get(name);
			if (result == null) {
				result = new Counter(name, name, 0L);
				subcounters.put(name, result);
			}
			return result;
		}

		/**
		 * Returns the number of counters in this group.
		 */
		public synchronized int size() {
			return subcounters.size();
		}

		



		public synchronized Iterator<Counter> iterator() {
			return new ArrayList<Counter>(subcounters.values()).iterator();
		}
	}

	// Map from group name (enum class name) to map of int (enum ordinal) to
	// counter record (name-value pair).
	private Map<String,Group> counters = new HashMap<String, Group>();

	private Map<Enum, Counter> cache = new IdentityHashMap<Enum, Counter>();

	public synchronized Collection<String> getGroupNames() {
		return counters.keySet();
	}

	public synchronized Iterator<Group> iterator() {
		return counters.values().iterator();
	}

	public synchronized Group getGroup(String groupName) {
		Group result = counters.get(groupName);
		if (result == null) {
			result = new Group(groupName);
			counters.put(groupName, result);
		}
		return result;
	}

	public synchronized Counter findCounter(Enum key) {
		Counter counter = cache.get(key);
		if (counter == null) {
			Group group = getGroup(key.getDeclaringClass().getName());
			counter = group.getCounterForName(key.toString());
			cache.put(key, counter);
		}
		return counter;    
	}

	public synchronized Counter findCounter(String group, String name) {
		return getGroup(group).getCounterForName(name);
	}

	@Deprecated
	public synchronized Counter findCounter(String group, int id, String name) {
		return getGroup(group).getCounterForName(name);
	}

	public synchronized void incrCounter(Enum key, long amount) {
		findCounter(key).value += amount;
	}

	public synchronized void incrCounter(String group, String counter, long amount) {
		getGroup(group).getCounterForName(counter).value += amount;
	}

	public synchronized long getCounter(Enum key) {
		return findCounter(key).value;
	}

	public synchronized void incrAllCounters(Counters other) {
		for (Group otherGroup: other) {
			Group group = getGroup(otherGroup.getName());
			group.displayName = otherGroup.displayName;
			for (Counter otherCounter : otherGroup) {
				Counter counter = group.getCounterForName(otherCounter.getName());
				counter.displayName = otherCounter.displayName;
				counter.value += otherCounter.value;
			}
		}
	}

	public static Counters sum(Counters a, Counters b) {
		Counters counters = new Counters();
		counters.incrAllCounters(a);
		counters.incrAllCounters(b);
		return counters;
	}

	public synchronized  int size() {
		int result = 0;
		for (Group group : this) {
			result += group.size();
		}
		return result;
	}

	

	

	

	/**
	 * Return textual representation of the counter values.
	 */
	public synchronized String toString() {
		StringBuilder sb = new StringBuilder("Counters: " + size());
		for (Group group: this) {
			sb.append("\n\t" + group.getDisplayName());
			for (Counter counter: group) {
				sb.append("\n\t\t" + counter.getDisplayName() + "=" + 
						counter.getCounter());
			}
		}
		return sb.toString();
	}

	/**
	 * Convert a counters object into a single line that is easy to parse.
	 * @return the string with "name=value" for each counter and separated by ","
	 */
	public synchronized String makeCompactString() {
		StringBuffer buffer = new StringBuffer();
		boolean first = true;
		for(Group group: this){   
			for(Counter counter: group) {
				if (first) {
					first = false;
				} else {
					buffer.append(',');
				}
				buffer.append(group.getDisplayName());
				buffer.append('.');
				buffer.append(counter.getDisplayName());
				buffer.append(':');
				buffer.append(counter.getCounter());
			}
		}
		return buffer.toString();
	}



	synchronized boolean contentEquals(Counters counters) {
		boolean isEqual = false;
		if (counters != null) {
			if (size() == counters.size()) {
				isEqual = true;
				for (Map.Entry<String, Group> entry : this.counters.entrySet()) {
					String key = entry.getKey();
					Group sourceGroup = entry.getValue();
					Group targetGroup = counters.getGroup(key);
					if (!sourceGroup.contentEquals(targetGroup)) {
						isEqual = false;
						break;
					}
				}
			}
		}
		return isEqual;
	}
}
