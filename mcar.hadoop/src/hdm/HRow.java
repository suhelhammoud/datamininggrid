package hdm;

import java.nio.ByteBuffer;

public class HRow {
	/**
	 * Size of long in bytes
	 */
	public static final int SIZEOF_HROW = (Long.SIZE + Integer.SIZE)
			/ Byte.SIZE;

	private byte[] bytes = new byte[SIZEOF_HROW];

	public static byte[] toBytes(long columnId, int rowId) {
		ByteBuffer bb = ByteBuffer.allocate(SIZEOF_HROW);
		bb.putLong(columnId);
		bb.putInt(rowId);
		return bb.array();
	}

	public HRow(byte[] bytes) {
		this.bytes = bytes;
	}

	public String toString() {
		return "" + getColumnId(bytes) + "," + getRowId(bytes);
	}

	public static long getColumnId(byte[] bytes) {
		ByteBuffer bb = ByteBuffer.wrap(bytes);
		return bb.getLong();
	}

	public static int getRowId(byte[] bytes) {
		ByteBuffer bb = ByteBuffer.wrap(bytes);
		return bb.getInt(Long.SIZE / Byte.SIZE);
	}

	public static void main(String[] args) {
		byte[] b = HRow.toBytes(55, 88);
		System.out.println(getColumnId(b));
		System.out.println(getRowId(b));
	}

}
