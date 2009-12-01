package hasim;

import org.apache.log4j.Logger;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

public class Tools {
	/**
	 * Logger for this class
	 */
	private static final Logger logger = Logger.getLogger(Tools.class);

	public static List<String> scater(Map<String, Integer> map) {
		logger.debug(map);

		List<String> r=new ArrayList<String>();
		for (Map.Entry<String, Integer> e : map.entrySet()) {
			for (int i = 0; i < e.getValue(); i++) {
				r.add(e.getKey());
			}
		}

		if(true)return r;
		//////////

		List<String> result = new ArrayList<String>();

		while (map.size() > 0) {
			for (Iterator<Map.Entry<String, Integer>> iter = map.entrySet()
					.iterator(); iter.hasNext();) {
				Map.Entry<String, Integer> m = iter.next();
				result.add(m.getKey());
				int v = m.getValue();
				if (v == 1)
					iter.remove();
				else
					m.setValue(v - 1);
			}
		}
		return result;
	}
}
