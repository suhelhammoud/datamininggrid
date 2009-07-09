package mcar.mapreduce;
public interface Reporter {
	public void setStatus(String s);
    public void progress();
    public Counters.Counter getCounter(String group, String name);
    public void incrCounter(Enum key, long amount);
    public void incrCounter(String group, String counter, long amount);
    //public InputSplit getInputSplit();
}
