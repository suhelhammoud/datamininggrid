package hasim;


public interface NetletUser {
	public void netletProgress(Netlet strm);
	public void netletComplete(Netlet strm);
	public void netletTransfere(int from, int to, Netlet strm);

}
