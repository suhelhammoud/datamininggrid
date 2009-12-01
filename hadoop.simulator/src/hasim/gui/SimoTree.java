package hasim.gui;

import org.apache.log4j.Logger;

import hasim.CPU;
import hasim.HDD;
import hasim.HMapper;
import hasim.HReducer;
import hasim.HTaskTracker;
import hasim.JobInfo;
import hasim.json.JsonRealRack;

import java.awt.BorderLayout;
import java.awt.GridBagConstraints;
import java.awt.GridBagLayout;
import java.awt.Insets;

import java.awt.Toolkit;
import java.util.LinkedHashMap;
import java.util.Map;

import javax.swing.JFrame;
import javax.swing.JPanel;
import javax.swing.JScrollPane;
import javax.swing.JTree;
import javax.swing.event.TreeSelectionEvent;
import javax.swing.event.TreeSelectionListener;
import javax.swing.tree.DefaultMutableTreeNode;
import javax.swing.tree.DefaultTreeModel;




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
public class SimoTree extends JPanel{
	/**
	 * Logger for this class
	 */
	private static final Logger logger = Logger.getLogger(SimoTree.class);

	public enum TreeIndex{mWaiting, mRunning, mFinished, rWaiting, rRunning, rFinished};

	Map<JobInfo, DefaultMutableTreeNode> jobs=new LinkedHashMap<JobInfo, DefaultMutableTreeNode>();
	private JScrollPane scrl;
	Map<HMapper, DefaultMutableTreeNode> mappers=new LinkedHashMap<HMapper, DefaultMutableTreeNode>();
	Map<HReducer, DefaultMutableTreeNode> reducers=new LinkedHashMap<HReducer, DefaultMutableTreeNode>();

	Map<Object, DefaultMutableTreeNode> nodes=new LinkedHashMap<Object, DefaultMutableTreeNode>();


	DefaultMutableTreeNode jobTracker = new DefaultMutableTreeNode("JobTracker");
	DefaultMutableTreeNode jw = new DefaultMutableTreeNode("Jobs Waiting");
	DefaultMutableTreeNode jr = new DefaultMutableTreeNode("Jobs Running");
	DefaultMutableTreeNode jf = new DefaultMutableTreeNode("Jobs Finished");
	
	
	DefaultMutableTreeNode rackes = new DefaultMutableTreeNode("Rackes");
	



	public DefaultTreeModel treeModel = new DefaultTreeModel(jobTracker);

	private void myInit(){
		jobTracker.insert(jw, jobTracker.getChildCount());
		jobTracker.insert(jr, jobTracker.getChildCount());
		jobTracker.insert(jf, jobTracker.getChildCount());
		
		jobTracker.add(rackes);


		nodes.put(jw, jw);
		nodes.put(jr,jr);
		nodes.put(jf, jf);
		
		
		
		
		
	}
	public void addRack(String rckname, Map<String, HTaskTracker> trackers) {
		DefaultMutableTreeNode rack = new DefaultMutableTreeNode(rckname);
		rackes.add(rack);
		for ( HTaskTracker  tracker : trackers.values()) {
			DefaultMutableTreeNode trackerNode = new DefaultMutableTreeNode(tracker);
			rack.add(trackerNode);
			
			DefaultMutableTreeNode cpuNode = new DefaultMutableTreeNode(tracker.cpu);
			DefaultMutableTreeNode hddNode = new DefaultMutableTreeNode(tracker.hdd);
			DefaultMutableTreeNode netNode = new DefaultMutableTreeNode(tracker.netend);
			DefaultMutableTreeNode infNode = new DefaultMutableTreeNode(
					"mappers:"+tracker.getaMappers()+ ", reducers:"+tracker.getaReducers());
			
			trackerNode.add(cpuNode);
			trackerNode.add(hddNode);
			trackerNode.add(netNode);
			trackerNode.add(infNode);
			
		}
		
	}

	

	public JTree tree;
	private Toolkit toolkit = Toolkit.getDefaultToolkit();


	public void addTest(String jobName){
		DefaultMutableTreeNode jobNode=new DefaultMutableTreeNode(jobName);

		DefaultMutableTreeNode mw=new DefaultMutableTreeNode("Mappers Waiting");
		DefaultMutableTreeNode mr=new DefaultMutableTreeNode("Mappers Running");
		DefaultMutableTreeNode mf=new DefaultMutableTreeNode("Mappers Finished");

		DefaultMutableTreeNode rw=new DefaultMutableTreeNode("Reducers Waiting");
		DefaultMutableTreeNode rr=new DefaultMutableTreeNode("Reducers Running");
		DefaultMutableTreeNode rf=new DefaultMutableTreeNode("Reducers Finished");

		jw.add(jobNode);

		jobNode.add(mw);
		jobNode.add(mr);
		jobNode.add(mf);

		jobNode.add(rw);
		jobNode.add(rr);
		jobNode.add(rf);

		nodes.put(jobNode, jobNode);
		nodes.put(mw, mw);
		nodes.put(mr, mr);
		nodes.put(mf, mf);
		nodes.put(rw, rw);
		nodes.put(rr, rr);
		nodes.put(rf, rf);

		for (int i = 0; i < 5; i++) {
			DefaultMutableTreeNode mNode=new DefaultMutableTreeNode("m_"+i);
			nodes.put("m__"+i, mNode);				
			mw.add(mNode);
		}
		for (int i = 0; i < 3; i++) {
			DefaultMutableTreeNode mNode=new DefaultMutableTreeNode("r_"+i);
			nodes.put("r__"+i, mNode);				
			mw.add(mNode);
		}

		tree.updateUI();

	}

	public void addJob(JobInfo job){
		DefaultMutableTreeNode jobNode=new DefaultMutableTreeNode(job);

		DefaultMutableTreeNode mw=new DefaultMutableTreeNode("Mappers Waiting");
		DefaultMutableTreeNode mr=new DefaultMutableTreeNode("Mappers Running");
		DefaultMutableTreeNode mf=new DefaultMutableTreeNode("Mappers Finished");

		DefaultMutableTreeNode rw=new DefaultMutableTreeNode("Reducers Waiting");
		DefaultMutableTreeNode rr=new DefaultMutableTreeNode("Reducers Running");
		DefaultMutableTreeNode rf=new DefaultMutableTreeNode("Reducers Finished");


		jw.add(jobNode);

		jobNode.add(mw);
		jobNode.add(mr);
		jobNode.add(mf);

		jobNode.add(rw);
		jobNode.add(rr);
		jobNode.add(rf);

		nodes.put(job, jobNode);
		//		nodes.put(mw, mw);
		//		nodes.put(mr, mr);
		//		nodes.put(mf, mf);
		//		nodes.put(rw, rw);
		//		nodes.put(rr, rr);
		//		nodes.put(rf, rf);

		for (HMapper mapper : job.mappersWaiting) {
			DefaultMutableTreeNode mNode=new DefaultMutableTreeNode(mapper);
			nodes.put(mapper, mNode);
			mw.add(mNode);
		}
		for (HReducer r : job.reducersWaiting) {
			DefaultMutableTreeNode rNode=new DefaultMutableTreeNode(r);
			nodes.put(r, rNode);
			rw.add(rNode);
		}
		tree.updateUI();

	}






	public SimoTree() {
		//init();
	}

	public void init(){
		myInit();
		initGUI();
	}
	

	public void moveNode(Object mapper, TreeIndex index){
		DefaultMutableTreeNode node=nodes.get(mapper);
		assert node != null;

		DefaultMutableTreeNode parentToRemove= (DefaultMutableTreeNode) node.getParent();
		int moveIndex=parentToRemove.getParent().getIndex(parentToRemove);
		
		DefaultMutableTreeNode parentToAdd=(DefaultMutableTreeNode) 
		node.getParent().getParent().getChildAt(moveIndex+1);

		
//
//		DefaultMutableTreeNode parentToAdd=(DefaultMutableTreeNode) 
//		node.getParent().getParent().getChildAt(index.ordinal());
//		
		


		parentToRemove.remove(node);
		parentToAdd.add(node);

		try {
			Thread.sleep(200);
		} catch (Exception e) {
			e.printStackTrace();
		}
		tree.updateUI();
	}

	private void initGUI() {
		try {
			{
				BorderLayout thisLayout = new BorderLayout();
				this.setLayout(thisLayout);
				this.setSize(300, 400);
				{
					scrl = new JScrollPane();
					this.add(scrl, BorderLayout.NORTH);
					{
						tree = new JTree(treeModel);
						scrl.getViewport().add(tree);
						//scrl.setViewportView(tree);
						BorderLayout treeLayout = new BorderLayout();
						tree.setLayout(treeLayout);
						tree.setSize(300, 400);
						tree.setEditable(true);
						tree.setAutoscrolls(true);
						tree.addTreeSelectionListener(new TreeSelectionListener() {
							public void valueChanged(TreeSelectionEvent evt) {
								treeValueChanged(evt);
							}
						});
					}
				}

			}
		} catch(Exception e) {
			e.printStackTrace();
		}
	}

	public static SimoTree createAndShowGUI() {
		//Create and set up the window.
		SimoTree panel = new SimoTree();

		JFrame frame = new JFrame("Frame title");
		frame.setLayout(new BorderLayout());
		frame.setDefaultCloseOperation(JFrame.EXIT_ON_CLOSE);

		//Create and set up the content pane.
		panel.setOpaque(true); //content panes must be opaque
		frame.setContentPane(panel);

		//Display the window.
		frame.pack();
		frame.setSize(300, 400);
		frame.setVisible(true);
		return panel;
	}

	public static void main(String[] args) throws Exception{
		SimoTree simotree=				createAndShowGUI();

		//		javax.swing.SwingUtilities.invokeLater(new Runnable() {
		//			public void run() {
		//				createAndShowGUI();
		//			}
		//		});

		Thread.sleep(500);

		simotree.addTest("job_1");

		Thread.sleep(10000);

		simotree.moveNode("m__2", TreeIndex.mFinished);

		Thread.sleep(10000);
	}
	
	private void treeValueChanged(TreeSelectionEvent evt) {
		DefaultMutableTreeNode node=(DefaultMutableTreeNode)tree.getLastSelectedPathComponent();
		Object o=node.getUserObject();
		logger.info("select:"+o );
		if( o instanceof HMapper){
			((HMapper) o).showMonitor();
		}else if (o instanceof HReducer){
			((HReducer) o).showMonitor();
		}else if (o instanceof HTaskTracker){
			logger.debug("select "+ o.toString());
		}else if( o instanceof CPU){
			((CPU) o).showMonitor();
		}else if( o instanceof HDD){
			logger.info(""+o);
		}

	}


	

}