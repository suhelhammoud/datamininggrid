package init;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;



public interface WritableComparable<T> extends Writable, Comparable<T> {
}

interface Writable {	  
	void write(DataOutput out) throws IOException;
	void readFields(DataInput in) throws IOException;
}