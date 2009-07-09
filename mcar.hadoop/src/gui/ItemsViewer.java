package gui;

import java.awt.BorderLayout;
import java.awt.event.ActionEvent;
import java.awt.event.ActionListener;

import javax.swing.JFileChooser;
import javax.swing.JMenu;
import javax.swing.JMenuBar;
import javax.swing.JMenuItem;
import javax.swing.JScrollPane;
import javax.swing.JTextArea;
import javax.swing.KeyStroke;

import javax.swing.WindowConstants;
import javax.swing.SwingUtilities;

import tries.MyColumn;


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
public class ItemsViewer extends javax.swing.JFrame {
	private JMenuBar mnubar;
	private JMenu jMenu1;
	private JMenuItem mnuFileOpen;
	private JMenuItem mnuClean;
	private JTextArea txt;
	private JScrollPane scrl;

	/**
	* Auto-generated main method to display this JFrame
	*/
	public static void main(String[] args) {
		SwingUtilities.invokeLater(new Runnable() {
			public void run() {
				ItemsViewer inst = new ItemsViewer();
				inst.setLocationRelativeTo(null);
				inst.setVisible(true);
			}
		});
	}
	
	public ItemsViewer() {
		super();
		initGUI();
	}
	
	private void initGUI() {
		try {
			setDefaultCloseOperation(WindowConstants.DISPOSE_ON_CLOSE);
			this.setTitle("Items Viewer");
			{
				scrl = new JScrollPane();
				getContentPane().add(scrl, BorderLayout.CENTER);
				{
					txt = new JTextArea();
					scrl.setViewportView(txt);
					txt.setText("Text");
				}
			}
			{
				mnubar = new JMenuBar();
				setJMenuBar(mnubar);
				{
					jMenu1 = new JMenu();
					mnubar.add(jMenu1);
					jMenu1.setText("File");
					{
						mnuFileOpen = new JMenuItem();
						jMenu1.add(mnuFileOpen);
						mnuFileOpen.setText("Open");
						mnuFileOpen.setAccelerator(KeyStroke.getKeyStroke("ctrl pressed O"));
						mnuFileOpen.addActionListener(new ActionListener() {
							public void actionPerformed(ActionEvent evt) {
								String fileName=choose("data/items");
								if(fileName ==null)return;
								MyColumn myColumn=new MyColumn();
								myColumn.read(fileName);
								txt.append("\nread directory "+ fileName);
								txt.append("\n"+ myColumn);
								
							}
						});
					}
					{
						mnuClean = new JMenuItem();
						jMenu1.add(mnuClean);
						mnuClean.setText("Clean");
						mnuClean.setAccelerator(KeyStroke.getKeyStroke("ctrl pressed D"));
						mnuClean.addActionListener(new ActionListener() {
							public void actionPerformed(ActionEvent evt) {
								txt.setText("");
							}
						});
					}
				}
			}
			pack();
			setSize(400, 300);
		} catch (Exception e) {
			e.printStackTrace();
		}
	}

	private String choose(String currentFile) {
		if (currentFile == null)
			currentFile = "data";
		JFileChooser fileChooser = new JFileChooser(currentFile);
		int result;
		fileChooser.setFileSelectionMode(JFileChooser.DIRECTORIES_ONLY);
		result = fileChooser.showOpenDialog(this);
		if (result == JFileChooser.APPROVE_OPTION) {
			String fileName = fileChooser.getSelectedFile().getAbsolutePath();
			return fileName;
		} else
			return null;
	}
}
