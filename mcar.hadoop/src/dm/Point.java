package dm;

import org.apache.log4j.Logger;

public class Point {
	/**
	 * Logger for this class
	 */
	private static final Logger logger = Logger.getLogger(Point.class);

	public final int x;
	public final int y;
	private int hashCode = 0;

	public Point(int x, int y) {
		this.x = x;
		this.y = y;
	}

	public Point(Point p) {
		this(p.x, p.y);
	}

	@Override
	public boolean equals(Object obj) {
		Point o = (Point) obj;
		return (x == o.x) && (y == o.y);
	}

	@Override
	public int hashCode() {
		if (hashCode == 0) {
			hashCode = 37 * x + y;
		}
		return hashCode;
	}

	@Override
	public String toString() {
		// TODO Auto-generated method stub
		return super.toString();
	}

}