package dfs;

public class Pair{
	enum Type{READ,WRTIE};
	Type type;
	double size;

	public Pair(Type type, double size) {
		super();
		this.type = type;
		this.size = size;
	}
	
	@Override
	public String toString() {
		// TODO Auto-generated method stub
		return "Pair("+type.name()+", "+ size+") ";
	}
}