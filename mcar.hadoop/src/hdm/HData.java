package hdm;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.TreeSet;

import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Scanner;
import org.apache.hadoop.hbase.io.BatchUpdate;
import org.apache.hadoop.hbase.io.RowResult;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.log4j.Logger;

import others.Tools;

import dm.Data;
import filters.IMap;

public class HData extends Data {
	/**
	 * Logger for this class
	 */
	private static final Logger logger = Logger.getLogger(HData.class);

	HTable hDataMap;
	HBaseConfiguration conf;
	int size = 0;

	public HData(HBaseConfiguration conf) {
		this.conf = conf;
	}

	public void create(String hdataName) {
		this.hDataMap = HUtil.create(conf, hdataName.getBytes());
	}

	public BatchUpdate addLine(int line, int[] items) {
		BatchUpdate bu = new BatchUpdate(Bytes.toBytes(line));
		bu.put(HUtil.FAMILY, HUtil.intArryToBytes(items));
		size++;
		return bu;
	}

	public int readFromFile(String fileName, String delima) {
		int result = -1;
		attributes.clear();
		dataMap.clear();
		imaps.clear();

		if (hDataMap == null)
			try {
				hDataMap = new HTable(conf, conf.get("hDataMap", "hDataMap")
						.getBytes());
			} catch (IOException e1) {
				e1.printStackTrace();
				return result;
			}

		int errorLine = 1;
		try {
			BufferedReader in = new BufferedReader(new FileReader(fileName));

			String s = "";
			while (!s.startsWith("@attribute")) {
				s = in.readLine().toLowerCase().trim();
			}

			// read till @data line
			int numOfCols = 0;
			while (!s.startsWith("@data")) {
				if (s.startsWith("@attribute")) {
					attributes.add(s.split("\\s+")[1]);// \\s+ one or more
														// spaces
					imaps.add(new IMap());
					numOfCols++;
				}
				s = in.readLine().toLowerCase().trim();
			}

			// read data
			int lineNumber = 1;
			while ((s = in.readLine()) != null) {

				final String[] arr = s.trim().split(",");

				int[] row = new int[arr.length];
				for (int i = 0; i < arr.length; i++) {
					row[i] = imaps.get(i).mapValue(arr[i], lineNumber);
				}
				hDataMap.commit(addLine(lineNumber, row));
				hDataMap.flushCommits();
				// dataMap.put(lineNumber++, row);
				errorLine++;
				lineNumber++;
			}
			result = lineNumber - 1;
			in.close();

		} catch (Exception e) {
			e.printStackTrace();
			logger.error("Error At Line " + errorLine);
		}
		return result;
	}

	@Override
	public int size() {
		return size;
	}

	@Override
	public Set<Integer> getLines() {
		Set<Integer> result = new TreeSet<Integer>();
		for (int i = 1, sz = size(); i <= sz; i++) {
			result.add(i);
		}
		return result;
	}

	@Override
	public String toString() {
		StringBuffer result = new StringBuffer("attributes: "
				+ Tools.join(attributes, " "));
		result.append("\ndata:");
		try {
			if (hDataMap == null)
				hDataMap = new HTable(conf, conf.get("hDataMap", "hDataMap")
						.getBytes());

			Scanner s = hDataMap.getScanner(new byte[][] { HUtil.FAMILY });
			try {
				for (RowResult r : s) {
					int line = Bytes.toInt(r.getRow());
					byte[] ba = r.get(HUtil.FAMILY).getValue();
					int[] bi = HUtil.byteArrayFromInts(ba);
					result.append("\n" + line + "\t" + Tools.join(bi, ","));
				}

			} finally {
				s.close();
			}
		} catch (IOException e1) {
			e1.printStackTrace();
			return "error";

		}

		return result.toString();

	}

	public static void main(String[] args) {
		HBaseConfiguration conf = new HBaseConfiguration();
		HData data = new HData(conf);
		data.create("hdata3");

		data.readFromFile("data/in/00.arff", ",");
		System.out.println(data.toString());

	}
}
