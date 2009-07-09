package dm;

public class LabelFrequency implements Comparable<LabelFrequency> {
	final int freq;
	String label = "default";
	final int intLabel;

	public int intLabel() {
		return intLabel;
	}

	public LabelFrequency(String label, int freq, int intLabel) {
		this.freq = freq;
		this.label = label;
		this.intLabel = intLabel;
	}

	public LabelFrequency(int freq, int intLabel) {
		this.freq = freq;
		this.intLabel = intLabel;
	}

	public void setLabel(String label) {
		this.label = label;
	}

	public int compareTo(LabelFrequency o) {
		// TODO added - for sorting
		int diff = freq - o.freq;
		if (diff != 0)
			return -diff;
		diff = intLabel - o.intLabel;
		if (diff != 0)
			return diff;
		return -label.compareTo(o.label);
	}

	@Override
	public String toString() {
		return label + "(" + intLabel + "):" + freq;
	}
}
