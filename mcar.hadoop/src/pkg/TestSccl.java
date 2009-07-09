package pkg;

import hdm.HRuleRank;

import java.io.IOException;
import java.io.Serializable;
import java.math.BigInteger;

import dm.Sccl;

public class TestSccl {

	/**
	 * @param args
	 * @throws ClassNotFoundException
	 */
	public static void main(String[] args) throws IOException,
			ClassNotFoundException {
		// TODO Auto-generated method stub
		Sccl sccl = new Sccl(3L);
		sccl.addLine(3, 4);
		sccl.addLine(4, 4);
		sccl.addLine(5, 55);

		sccl.addLine(6, 55);

		System.out.println(sccl);

		byte[] bytes = sccl.toBytes_old();
		System.out.println("size =" + bytes.length);

		sccl.addLine(6, 66);

		System.out.println(Sccl.fromBytes(bytes).toString());

		HRuleRank rr1 = new HRuleRank(10, 3, 0.5, 4, 100);
		HRuleRank rr2 = new HRuleRank(10, 4, 0.6, 4, 101);

		byte[] b1 = rr1.toBytes3();
		byte[] b2 = rr2.toBytes3();

	}

}
