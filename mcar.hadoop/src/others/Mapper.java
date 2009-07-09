package others;

import org.apache.log4j.Logger;
import org.apache.log4j.helpers.FileWatchdog;

 
import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.FileReader;
import java.io.FileWriter;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import junit.framework.TestResult;

import filters.IMap;

public class Mapper {
	/**
	 * Logger for this class
	 */
	private static final Logger logger = Logger.getLogger(Mapper.class);

	List<String> lines = new ArrayList<String>();
	List<List<String>> attributes = new ArrayList<List<String>>();
	List<List<Double>> ranges = new ArrayList<List<Double>>();

	public boolean readFromFile(String fileName) {
		lines.clear();
		try {
			BufferedReader in = new BufferedReader(new FileReader(fileName));
			String s = "";
			while ((s = in.readLine()) != null) {
				s = s.toLowerCase().trim();
				if (s.startsWith("@attribute")) {
					lines.add(s);
				} else if (s.startsWith("#") || s.equals("")
						|| s.startsWith("@relation"))
					continue;
				else if (s.startsWith("@data"))
					break;
				else
					logger.error("check this case on line:" + s);
			}
			in.close();
		} catch (Exception e) {
			e.printStackTrace();
		}
		return true;
	}

	public void build() {
		for (String line : lines) {
			attributes.add(getRegex(line));
			ranges.add(getRanges(line));
		}
	}

	public static List<String> getRegex(String reg, String line) {
		List<String> result = new ArrayList<String>();
		Pattern pattern = Pattern.compile(reg);
		Matcher matcher = pattern.matcher(line);
		while (matcher.find()) {
			String value = matcher.group();
			result.add(value);
		}
		return result;
	}

	public static List<String> getRegex(String line) {
		// get the item
		String itemPattern = "'\\\\'.+?\\\\''";
		return getRegex(itemPattern, line);
	}

	public static List<Double> getRanges(String line) {
		List<Double> result = new ArrayList<Double>();
		// get the range
		String regexRange = "--?\\d+.?\\d*\\]";
		List<String> items = getRegex(regexRange, line);
		for (String itm : items) {
			Double d = Double.parseDouble(parse(itm));
			result.add(d);
		}
		return result;
	}

	/**
	 * 
	 * @param number
	 *            example : --4.60]
	 * @return -4.60
	 */
	public static String parse(String number) {
		String result = number;
		result = number.substring(1);
		return result.substring(0, result.length() - 1);
	}

	@Override
	public String toString() {
		StringBuffer result = new StringBuffer();
		for (int i = 0; i < lines.size(); i++) {
			result.append("\nline:" + lines.get(i));
			result.append("\nitems: " + attributes.get(i));
			result.append("\nranges: " + ranges.get(i) + "\n");
		}
		return result.toString();
	}

	/**
	 * 
	 * @param inFile
	 *            file contains numeric
	 * @param outFile
	 *            file contains nominals
	 */
	public void apply(String inFile, String outFile) {
		List<Integer> numerics = new ArrayList<Integer>();
		try {
			BufferedReader in = new BufferedReader(new FileReader(inFile));
			BufferedWriter out = new BufferedWriter(new FileWriter(outFile));

			String s = in.readLine().toLowerCase().trim();
			;
			while (!s.startsWith("@attribute")) {
				out.write(s + "\n");
				s = in.readLine().toLowerCase().trim();
			}
			int numOfAtts = 0;
			while (!s.startsWith("@data")) {
				if (!s.startsWith("@attribute")) {
					out.write(s + "\n");
					s = in.readLine().toLowerCase().trim();
					continue;
				}

				if (!s.contains(" numeric") || !s.contains(" real")) {
					out.write(s + "\n");
				} else {
					numerics.add(numOfAtts);
					String att = lines.get(numOfAtts);
					out.write(att + "\n");
				}
				s = in.readLine().toLowerCase().trim();
				numOfAtts++;

			}
			out.write("@data\n");

			while ((s = in.readLine()) != null) {
				s = s.toLowerCase().trim();
				out.write(mapLine(numerics, s) + "\n");
			}
			in.close();
			out.close();
		} catch (Exception e) {
			e.printStackTrace();
		}
	}

	private String mapLine(List<Integer> numerics, String line) {
		StringBuffer result = new StringBuffer();
		String[] items = line.split(",");
		for (Integer n : numerics) {
			double d = Double.parseDouble(items[n]);
			List<Double> range = ranges.get(n);
			int location = 0;
			for (location = 0; location < range.size(); location++) {
				if (d < range.get(location))
					break;
			}
			items[n] = attributes.get(n).get(location);
		}
		result.append(items[0]);
		for (int i = 1; i < items.length; i++) {
			result.append("," + items[i]);
		}
		return result.toString();
	}

	public static void main(String[] args) {
		Mapper m = new Mapper();
		m.readFromFile("data/weather_un_disc.arff");
		m.build();
		logger.info(m);

		List<Integer> numerics = Arrays.asList(new Integer[] { 1, 2 });
		String line = "sunny,85,85,FALSE,yes";
		logger.info(m.mapLine(numerics, line));

		m.apply("data/weather_in.arff", "data/weather_out.arff");

		// Mapper m=new Mapper();
		// m.readFromFile(fileName);
		// m.build();
		// m.apply(inFile, outFile);

	}
}
