package hasim;

public class Test {
	
	public static void main(String[] args) {
		System.out.println(Long.MAX_VALUE);
		int jobs=19;
		int cores=4;
		int index=0;
		for (int i = 0; i < 10; i++) {
			for (int j = 0; j < cores; j++) {
				int inner=(index+j)%jobs;
				System.out.print(inner+"\t");
			}
			System.out.println();
			index=(index+ cores)% jobs;

		}
	}

}
