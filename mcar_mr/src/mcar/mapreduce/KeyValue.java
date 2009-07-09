package mcar.mapreduce;

public class KeyValue implements Comparable{
	Comparable key;
	Object value;
	public KeyValue(Comparable key2, Object value2) {
		this.key=key2;
		this.value=value2;
	}

	public KeyValue(KeyValue kv) {
		this((Comparable) kv.key,kv.value);
	}
	@Override
	public int compareTo(Object o) {
		KeyValue other=(KeyValue)o;
		return key.compareTo(other.key);
	}
	
	@Override
	public String toString() {
		return "<"+key.toString()+", "+value.toString()+">";
	}
}
