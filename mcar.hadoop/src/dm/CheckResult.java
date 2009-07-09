package dm;

import java.util.Set;

public class CheckResult {
	public final int label;
	public final Set<Integer> trueCovered;
	public final Set<Integer> falseCoverd;
	Set<Integer> notCovered;

	public CheckResult(int label, Set<Integer> trueCoverd,
			Set<Integer> falseCoverd) {
		this.label = label;
		this.trueCovered = trueCoverd;
		this.falseCoverd = falseCoverd;
	}

	public CheckResult(int label, Set<Integer> trueCoverd,
			Set<Integer> falseCoverd, Set<Integer> notCovered) {
		this(label, trueCoverd, falseCoverd);
		this.notCovered = notCovered;
	}

}
