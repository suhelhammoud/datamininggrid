package hasim.gui;
import java.awt.event.ActionEvent;
import java.awt.event.ActionListener;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

import javax.swing.JButton;

import javax.swing.WindowConstants;
import javax.swing.SwingUtilities;


/**
* This code was edited or generated using CloudGarden's Jigloo
* SWT/Swing GUI Builder, which is free for non-commercial
* use. If Jigloo is being used commercially (ie, by a corporation,
* company or business for any purpose whatever) then you
* should purchase a license for each developer using Jigloo.
* Please visit www.cloudgarden.com for details.
* Use of Jigloo implies acceptance of these licensing terms.
* A COMMERCIAL LICENSE HAS NOT BEEN PURCHASED FOR
* THIS MACHINE, SO JIGLOO OR THIS CODE CANNOT BE USED
* LEGALLY FOR ANY CORPORATE OR COMMERCIAL PURPOSE.
*/
public class NewJFrame extends javax.swing.JFrame {
	private JButton btnLock;
	private JButton btnUnlock;
	
	Lock lock=new ReentrantLock();

	/**
	* Auto-generated main method to display this JFrame
	*/
	public static void main(String[] args) {
		SwingUtilities.invokeLater(new Runnable() {
			public void run() {
				NewJFrame inst = new NewJFrame();
				inst.setLocationRelativeTo(null);
				inst.setVisible(true);
			}
		});
	}
	
	public NewJFrame() {
		super();
		initGUI();
	}
	
	public void panelWait(){
		lock.lock();
		System.out.println("get the lock");
		lock.unlock();
		System.out.println("back to main thread");

		
	}
	private void initGUI() {
		try {
			setDefaultCloseOperation(WindowConstants.DISPOSE_ON_CLOSE);
			getContentPane().setLayout(null);
			{
				btnLock = new JButton();
				getContentPane().add(btnLock);
				btnLock.setText("lock");
				btnLock.setBounds(51, 44, 89, 21);
				btnLock.addActionListener(new ActionListener() {
					public void actionPerformed(ActionEvent evt) {
						btnLockActionPerformed(evt);
					}
				});
			}
			{
				btnUnlock = new JButton();
				getContentPane().add(btnUnlock);
				btnUnlock.setText("unlock");
				btnUnlock.setBounds(169, 44, 68, 21);
				btnUnlock.addActionListener(new ActionListener() {
					public void actionPerformed(ActionEvent evt) {
						btnUnlockActionPerformed(evt);
					}
				});
			}
			pack();
			setSize(400, 300);
		} catch (Exception e) {
			e.printStackTrace();
		}
	}
	
	private void btnLockActionPerformed(ActionEvent evt) {
		System.out.println("btnLock.actionPerformed, event="+evt);
		lock.lock();
	}
	
	private void btnUnlockActionPerformed(ActionEvent evt) {
		System.out.println("btnUnlock.actionPerformed, event="+evt);
		lock.unlock();
	}

}
