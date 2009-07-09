package filters;

import org.apache.log4j.*;
import java.util.*;

public class IMap {
	static Logger log = Logger.getLogger(IMap.class);

	private Map<String, Integer> imap = new TreeMap<String, Integer>();
	private Map<Integer, String> rmap = new TreeMap<Integer, String>();

	/**
	 * return the first line which has the same value
	 * 
	 * @param value
	 * @param line
	 * @return
	 */
	public Integer mapValue(final String value, final Integer line) {
		Integer ln = imap.get(value);
		if (ln != null) {
			if (ln < line)
				return ln;
			else {
				log.error("line " + line + " must be greater than "
						+ ln.doubleValue());
				return -1;
			}
		} else {
			Integer lastLine = imap.put(value, line);
			String lastString = rmap.put(line, value);
			if (lastLine != null || lastString != null) {
				log.error("line " + lastLine + ", value " + lastString);
			}
			return line;
		}

	}

	public Integer get(String key) {
		return imap.get(key);
	}

	public String getString(Integer i) {
		return rmap.get(i);
	}

	// TODO check if the mapping is correct
	public Set<Integer> intKeySet() {
		return (Set<Integer>) imap.values();
	}

	public Set<String> StringKeySet() {
		return imap.keySet();
	}

	public Set<Integer> getIntegerSet() {
		return rmap.keySet();
	}

	public Set<String> getStringSet() {
		return imap.keySet();
	}

	@Override
	public String toString() {

		return rmap.toString();
	}

}