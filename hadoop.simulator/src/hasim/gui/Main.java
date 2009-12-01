package hasim.gui;


import org.apache.log4j.Logger;

import gridsim.GridSim;
import hasim.HJobTracker;
import hasim.HMapper;
import hasim.HReducer;
import hasim.JobInfo;
import hasim.gui.HMonitor.DebugMode;
import hasim.json.JsonConfig;
import hasim.json.JsonJob;
import java.awt.BorderLayout;
import java.awt.GridBagConstraints;
import java.awt.GridBagLayout;
import java.awt.Insets;

import java.awt.event.ActionEvent;
import java.awt.event.ActionListener;
import java.io.File;
import java.io.IOException;
import javax.swing.ButtonGroup;
import javax.swing.JButton;
import javax.swing.JCheckBoxMenuItem;

import javax.swing.JFileChooser;
import javax.swing.JMenu;
import javax.swing.JMenuBar;
import javax.swing.JMenuItem;
import javax.swing.JPanel;
import javax.swing.JRadioButtonMenuItem;
import javax.swing.JScrollPane;
import javax.swing.JSeparator;
import javax.swing.JTextArea;
import javax.swing.KeyStroke;

import javax.swing.WindowConstants;
import javax.swing.SwingUtilities;
import javax.xml.stream.events.StartDocument;

import org.codehaus.jackson.map.ObjectMapper;

import eduni.simjava.Sim_system;


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
public class Main extends javax.swing.JFrame {
	/**
	 * Logger for this class
	 */
	private static final Logger logger = Logger.getLogger(Main.class);

	private JMenuBar mnubar;
	private JMenuItem mnuOpenFile;
	private JMenuItem mnuJob;
	private JMenuItem mnuTestOpen;
	public SimoTree simoPanel;
	private JMenuItem mnuClear;
	private JMenu mnuInfo;
	private JTextArea txt;
	private JMenuItem mnuSimStart;
	private JScrollPane scrl;
	private JMenuItem mnuSimStop;
	private JCheckBoxMenuItem mnuModeSleep;
	private JCheckBoxMenuItem mnuModeStep;
	private JSeparator sep1;
	private JMenu mnuSimulation;
	private JMenu File;

	public HJobTracker jobTracker=null;
	/**
	* Auto-generated main method to display this JFrame
	*/
	public static void main(String[] args) {
		System.out.println("start");
		try {
			Workbench.initGridSim();

			Workbench.initSimulator();
			Workbench.jobtracker.monitor.setVisible(true);
			
			Main inst = new Main();
			inst.setVisible(true);
			
			inst.jobTracker=Workbench.jobtracker;
			
			Workbench.jobtracker.simoTree=inst.simoPanel;
			
			Workbench.startGridSim();
			
		} catch (Exception e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}
	
	public Main() {
		super();
		initGUI();
	
	}
	
	private void initGUI() {
		try {
			setDefaultCloseOperation(WindowConstants.DISPOSE_ON_CLOSE);
			GridBagLayout thisLayout = new GridBagLayout();
			getContentPane().setLayout(thisLayout);
			{
				scrl = new JScrollPane();
				getContentPane().add(scrl, new GridBagConstraints(2, 0, 2, 4, 0.0, 0.0, GridBagConstraints.CENTER, GridBagConstraints.BOTH, new Insets(0, 0, 0, 0), 0, 0));
				{
					txt = new JTextArea();
					scrl.setViewportView(txt);
					txt.setText("txt");
					txt.setPreferredSize(new java.awt.Dimension(190, 245));
				}
			}
			{
				simoPanel = new SimoTree();
				BorderLayout simoPanelLayout = new BorderLayout();
				getContentPane().add(simoPanel, new GridBagConstraints(0, 0, 2, 4, 0.0, 0.0, GridBagConstraints.CENTER, GridBagConstraints.BOTH, new Insets(0, 0, 0, 0), 0, 0));
				simoPanel.setLayout(simoPanelLayout);
				simoPanel.init();
			}
			thisLayout.rowWeights = new double[] {0.1, 0.1, 0.1, 0.1};
			thisLayout.rowHeights = new int[] {7, 7, 7, 7};
			thisLayout.columnWeights = new double[] {0.1, 0.1, 0.1, 0.1};
			thisLayout.columnWidths = new int[] {7, 7, 7, 7};
			{
				mnubar = new JMenuBar();
				setJMenuBar(mnubar);
				{
					File = new JMenu();
					mnubar.add(File);
					File.setText("File");
					{
						mnuOpenFile = new JMenuItem();
						File.add(mnuOpenFile);
						mnuOpenFile.setText("Open");
						mnuOpenFile.setAccelerator(KeyStroke.getKeyStroke("ctrl pressed O"));
						mnuOpenFile.addActionListener(new ActionListener() {
							public void actionPerformed(ActionEvent evt) {
								mnuOpenFileActionPerformed(evt);
							}
						});
					}
					{
						mnuTestOpen = new JMenuItem();
						File.add(mnuTestOpen);
						mnuTestOpen.setText("Test Batch Jobs");
						mnuTestOpen.setBounds(48, 19, 118, 23);
						mnuTestOpen.setAccelerator(KeyStroke.getKeyStroke("ctrl pressed P"));
						mnuTestOpen.addActionListener(new ActionListener() {
							public void actionPerformed(ActionEvent evt) {
								mnuTestOpenActionPerformed(evt);
							}
						});
					}
				}
				{
					mnuSimulation = new JMenu();
					mnubar.add(mnuSimulation);
					mnuSimulation.setText("Simulation");
					{
						mnuSimStart = new JMenuItem();
						mnuSimulation.add(mnuSimStart);
						mnuSimStart.setText("Start");
						mnuSimStart.addActionListener(new ActionListener() {
							public void actionPerformed(ActionEvent evt) {
								mnuSimStartActionPerformed(evt);
							}
						});
					}
					{
						mnuSimStop = new JMenuItem();
						mnuSimulation.add(mnuSimStop);
						mnuSimStop.setText("Stop");
						mnuSimStop.setAccelerator(KeyStroke.getKeyStroke("ctrl pressed S"));
						mnuSimStop.addActionListener(new ActionListener() {
							public void actionPerformed(ActionEvent evt) {
								mnuSimStopActionPerformed(evt);
							}
						});
					}
					{
						sep1 = new JSeparator();
						mnuSimulation.add(sep1);
						sep1.setBounds(48, 21, 70, 7);
					}
					
					{
						mnuModeSleep = new JCheckBoxMenuItem();
						mnuSimulation.add(mnuModeSleep);
						mnuModeSleep.setText("Sleep");
						mnuModeSleep.setSelected(true);
						mnuModeSleep.addActionListener(new ActionListener() {
							public void actionPerformed(ActionEvent evt) {
								mnuModeSleepActionPerformed(evt);
							}
						});
					}
					{
						mnuModeStep = new JCheckBoxMenuItem();
						mnuSimulation.add(mnuModeStep);
						mnuModeStep.setText("Step");
						mnuModeStep.setSelected(true);
						mnuModeStep.addActionListener(new ActionListener() {
							public void actionPerformed(ActionEvent evt) {
								mnuModeSleepActionPerformed(evt);							}
						});
					}
				}
				{
					mnuInfo = new JMenu();
					mnubar.add(mnuInfo);
					mnuInfo.setText("Info");
					{
						mnuJob = new JMenuItem();
						mnuInfo.add(mnuJob);
						mnuJob.setText("Info Job");
						mnuJob.addActionListener(new ActionListener() {
							public void actionPerformed(ActionEvent evt) {
								mnuJobActionPerformed(evt);
							}
						});
					}
					{
						mnuClear = new JMenuItem();
						mnuInfo.add(mnuClear);
						mnuClear.setText("Clear");
						mnuClear.setAccelerator(KeyStroke.getKeyStroke("ctrl pressed L"));
						mnuClear.addActionListener(new ActionListener() {
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
	
	private void mnuOpenFileActionPerformed(ActionEvent evt) {
		JFileChooser chooser=new JFileChooser("data/json");
		int ack=chooser.showOpenDialog(this);
		if(ack != JFileChooser.APPROVE_OPTION)return;
		JsonJob job=null;
		try {
			//"data/json/job.json"
			//job=(JsonJob)JsonConfig.read("data/json/job.json", JsonJob.class);
			job=(JsonJob)JsonConfig.read(chooser.getSelectedFile().getAbsolutePath(), JsonJob.class);
		} catch (Exception e) {
			e.printStackTrace();
		}
		//logger.info("read job "+ job);
		jobTracker.addJob(job);
		//TODO add your code for mnuOpenFile.actionPerformed
	}
	
	private void mnuModeSleepActionPerformed(ActionEvent evt) {
		boolean sleep=mnuModeSleep.isSelected();
		boolean step=mnuModeStep.isSelected();
		
		if(sleep && step){
			HMonitor.debugMode=DebugMode.SLEEP_STEP;
			return;
		}
		if(sleep){
			HMonitor.debugMode=DebugMode.SLEEP;
			return;
		}
		if(step){
			HMonitor.debugMode=DebugMode.STEP;
			return;
		}
		HMonitor.debugMode=DebugMode.NONE;
	}
	
	Workbench workbench=null;
	private void mnuSimStartActionPerformed(ActionEvent evt) {
			try {
				Workbench.initGridSim();

				Workbench.initSimulator();
				workbench.jobtracker.monitor.setVisible(true);
				Workbench.startGridSim();
				
			} catch (Exception e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
	}
	
	private void mnuSimStopActionPerformed(ActionEvent evt) {
		System.out.println("mnuSimStop.actionPerformed, event="+evt);
		//Workbench.stopSimulation();
	}
	
	private void mnuJobActionPerformed(ActionEvent evt) {
		if(Workbench.jobtracker ==null)return;
		
		for (JobInfo job : Workbench.jobtracker.jobsRunning) {
			for (HMapper mapper : job.mappersRunning) {
				txt.append("\n"+mapper.info());
			}
			
			for (HReducer reducer : job.reducersRunning) {
				txt.append("\n"+reducer.info());
			}
		} 
		for (JobInfo job : Workbench.jobtracker.jobsFinished) {
			for (HMapper mapper : job.mappersFinished) {
				txt.append("\n"+mapper.info());
			}
			
			for (HReducer reducer : job.reducersFinished) {
				txt.append("\n"+reducer.info());
			}
		} 
	}
	
	public void log(String s){
		txt.append("\n"+s);
	}
	
	private void mnuTestOpenActionPerformed(ActionEvent evt) {
		JsonJob job=null;
		try {
			job=(JsonJob)JsonConfig.read("data/json/job.json", JsonJob.class);
		} catch (Exception e) {
			e.printStackTrace();
		}
		jobTracker.addJob(job);
	}

}
