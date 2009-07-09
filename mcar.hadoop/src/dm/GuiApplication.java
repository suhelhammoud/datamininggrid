package dm;

import org.apache.log4j.Logger;

import java.awt.BorderLayout;
import java.awt.event.ActionEvent;
import java.awt.event.ActionListener;
import java.math.BigInteger;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;

import javax.swing.AbstractAction;
import javax.swing.JButton;
import javax.swing.JFileChooser;
import javax.swing.JMenu;
import javax.swing.JMenuBar;
import javax.swing.JMenuItem;
import javax.swing.JScrollPane;
import javax.swing.JSeparator;
import javax.swing.JTextArea;
import javax.swing.JTextField;
import javax.swing.JToolBar;
import javax.swing.KeyStroke;
import javax.swing.SwingUtilities;
import javax.swing.WindowConstants;

import others.Tools;

/**
 * This code was edited or generated using CloudGarden's Jigloo SWT/Swing GUI
 * Builder, which is free for non-commercial use. If Jigloo is being used
 * commercially (ie, by a corporation, company or business for any purpose
 * whatever) then you should purchase a license for each developer using Jigloo.
 * Please visit www.cloudgarden.com for details. Use of Jigloo implies
 * acceptance of these licensing terms. A COMMERCIAL LICENSE HAS NOT BEEN
 * PURCHASED FOR THIS MACHINE, SO JIGLOO OR THIS CODE CANNOT BE USED LEGALLY FOR
 * ANY CORPORATE OR COMMERCIAL PURPOSE.
 */
public class GuiApplication extends javax.swing.JFrame {
	/**
	 * Logger for this class
	 */
	private static final Logger logger = Logger.getLogger(GuiApplication.class);

	{
		// Set Look & Feel
		try {
			javax.swing.UIManager.setLookAndFeel(javax.swing.UIManager.getSystemLookAndFeelClassName());
		} catch (Exception e) {
			e.printStackTrace();
		}
	}

	private JMenuBar mnubar;
	private JMenu mnuFile;
	private JMenuItem mnuOpenArff;
	private JMenuItem mnuBuildMapReduce;
	private JMenu mnuMapReduce;
	private JMenuItem mnuBuildMClassifier;
	private JMenuItem mnuMBuildClassifier;
	private JMenu mnuMClassifier;
	private JMenuItem mnuClearLocalVariables;
	private JMenuItem mnuBuildMultiClassifier;
	private JSeparator sep5;
	private JMenuItem mnuPrintSelfPredicted;
	private JSeparator sep3;
	private JMenuItem mnuCleanItem;
	private JMenu mnuClean;
	private JMenuItem mnueBuildClassifier;
	private JTextField txtIteration;
	private JButton btn;
	private JTextField txtConfidence;
	private JTextArea txt;
	private JScrollPane scrl;
	private JTextField txtSupport;
	private JToolBar toolbar;
	private JSeparator sep2;
	private JMenuItem mnuPrintRules;
	private JMenuItem mnuPrintCandidateRules;
	private JMenuItem mnuPrintColumns;
	private JSeparator sep1;
	private JMenuItem mnuPrintClassifier;
	private JMenuItem mnuBuildClassifier;
	private JMenu mnuClassifier;
	private JMenuItem mnuPrintTest;
	private JMenuItem mnuPrintTraining;
	private JMenuItem mnuNewTestData;
	private JMenuItem mnuNewTraining;
	private JMenu mnuData;
	private JMenuItem mnuExit;
	private JMenuItem mnuSaveFile;

	Map<String, String> map = new TreeMap<String, String>();
	Map<String, Object> object = new TreeMap<String, Object>();

	/**
	 * Auto-generated main method to display this JFrame
	 */
	public static void main(String[] args) {
		SwingUtilities.invokeLater(new Runnable() {
			public void run() {
				GuiApplication inst = new GuiApplication();
				inst.setLocationRelativeTo(null);
				inst.setVisible(true);
			}
		});
	}

	public GuiApplication() {
		super();
		initGUI();
	}

	private void initGUI() {
		try {
			setDefaultCloseOperation(WindowConstants.DISPOSE_ON_CLOSE);
			{
				toolbar = new JToolBar();
				getContentPane().add(toolbar, BorderLayout.NORTH);
				{
					txtSupport = new JTextField();
					toolbar.add(txtSupport);
					txtSupport.setText("0.02");
					txtSupport.setPreferredSize(new java.awt.Dimension(75, 21));
				}
				{
					txtConfidence = new JTextField();
					toolbar.add(txtConfidence);
					txtConfidence.setText("0.40");
					txtConfidence.setPreferredSize(new java.awt.Dimension(86,
							21));
				}
				{
					btn = new JButton();
					toolbar.add(btn);
					btn.setText("Build");
					btn.setPreferredSize(new java.awt.Dimension(151, 21));
				}
				{
					txtIteration = new JTextField();
					toolbar.add(txtIteration);
					txtIteration.setText("1");
				}
			}
			{
				scrl = new JScrollPane();
				getContentPane().add(scrl);
				scrl.setPreferredSize(new java.awt.Dimension(32, 20));
				{
					txt = new JTextArea();
					scrl.setViewportView(txt);
					txt.setText("txt\n");
				}
			}
			{
				mnubar = new JMenuBar();
				setJMenuBar(mnubar);
				{
					mnuFile = new JMenu();
					mnubar.add(mnuFile);
					mnuFile.setText("File");
					{
						mnuOpenArff = new JMenuItem();
						mnuFile.add(mnuOpenArff);
						mnuOpenArff.setText("Open File");
					}
					{
						mnuSaveFile = new JMenuItem();
						mnuFile.add(mnuSaveFile);
						mnuSaveFile.setText("Save File");
					}
					{
						mnuExit = new JMenuItem();
						mnuFile.add(mnuExit);
						mnuExit.setText("Exit");
					}
				}
				{
					mnuData = new JMenu();
					mnubar.add(mnuData);
					mnuData.setText("Data");
					{
						mnuNewTraining = new JMenuItem();
						mnuData.add(mnuNewTraining);
						mnuNewTraining.setText("New Training Data");
						mnuNewTraining.setAccelerator(KeyStroke
								.getKeyStroke("ctrl pressed O"));
						mnuNewTraining.addActionListener(new ActionListener() {
							public void actionPerformed(ActionEvent evt) {
								String train = choose(map.get("train"));
								if (train != null)
									map.put("train", train);
							}
						});

					}
					{
						mnuNewTestData = new JMenuItem();
						mnuData.add(mnuNewTestData);
						mnuNewTestData.setText("New Test Data");
						mnuNewTestData.addActionListener(new ActionListener() {
							public void actionPerformed(ActionEvent evt) {
								String test = choose(map.get("test"));
								if (test != null)
									map.put("test", test);
							}
						});
					}
					{
						sep2 = new JSeparator();
						mnuData.add(sep2);
					}
					{
						mnuPrintTraining = new JMenuItem();
						mnuData.add(mnuPrintTraining);
						mnuPrintTraining.setText("Print Training");
						mnuPrintTraining
								.addActionListener(new ActionListener() {
									public void actionPerformed(ActionEvent evt) {
										String train = map.get("train");
										if (train == null)
											return;
										Data data = new Data(train);
										txt.append("\n" + data.toString());

									}
								});
					}
					{
						mnuPrintTest = new JMenuItem();
						mnuData.add(mnuPrintTest);
						mnuPrintTest.setText("Print Test");
						mnuPrintTest.addActionListener(new ActionListener() {
							public void actionPerformed(ActionEvent evt) {
								String test = map.get("test");
								if (test == null)
									return;
								Data data = new Data(test);
								txt.append("\n" + data.toString());
							}
						});
					}
					{
						sep3 = new JSeparator();
						mnuData.add(sep3);
						sep3.setBounds(110, 90, 70, 8);
					}
					{
						mnuPrintSelfPredicted = new JMenuItem();
						mnuData.add(mnuPrintSelfPredicted);
						mnuPrintSelfPredicted.setText("Print Self Predicted");
						mnuPrintSelfPredicted
								.addActionListener(new ActionListener() {
									public void actionPerformed(ActionEvent evt) {
										mnuPrintSelfPredictedActionPerformed(evt);
									}
								});
					}
				}
				{
					mnuClassifier = new JMenu();
					mnubar.add(mnuClassifier);
					mnuClassifier.setText("Classifier");
					{
						mnueBuildClassifier = new JMenuItem();
						mnuClassifier.add(mnueBuildClassifier);
						mnueBuildClassifier.setText("Build Classifier");
						mnueBuildClassifier.setAccelerator(KeyStroke
								.getKeyStroke("ctrl pressed B"));
						mnueBuildClassifier
								.addActionListener(new ActionListener() {
									public void actionPerformed(ActionEvent evt) {
										mnueBuildClassifierActionPerformed(evt);
									}
								});
					}

					{
						mnuPrintClassifier = new JMenuItem();
						mnuClassifier.add(mnuPrintClassifier);
						mnuPrintClassifier.setText("Print Classififier");
						mnuPrintClassifier
								.addActionListener(new ActionListener() {
									public void actionPerformed(ActionEvent evt) {
										mnuPrintClassifierActionPerformed(evt);
									}
								});
					}
					{
						sep1 = new JSeparator();
						mnuClassifier.add(sep1);
					}
					{
						mnuPrintColumns = new JMenuItem();
						mnuClassifier.add(mnuPrintColumns);
						mnuPrintColumns.setText("Print Columns");
						mnuPrintColumns.addActionListener(new ActionListener() {
							public void actionPerformed(ActionEvent evt) {
								mnuPrintColumnsActionPerformed(evt);
							}
						});
					}
					{
						mnuPrintCandidateRules = new JMenuItem();
						mnuClassifier.add(mnuPrintCandidateRules);
						mnuPrintCandidateRules.setText("Print Candidate Rules");
						mnuPrintCandidateRules
								.addActionListener(new ActionListener() {
									public void actionPerformed(ActionEvent evt) {
										mnuPrintCandidateRulesActionPerformed(evt);
									}
								});
					}
					{
						mnuPrintRules = new JMenuItem();
						mnuClassifier.add(mnuPrintRules);
						mnuPrintRules.setText("Print Rules");
						mnuPrintRules.addActionListener(new ActionListener() {
							public void actionPerformed(ActionEvent evt) {
								mnuPrintRulesActionPerformed(evt);
							}
						});
					}
					{
						sep5 = new JSeparator();
						mnuClassifier.add(sep5);
						sep5.setBounds(125, 111, 70, 8);
					}
					{
						mnuBuildMultiClassifier = new JMenuItem();
						mnuClassifier.add(mnuBuildMultiClassifier);
						mnuBuildMultiClassifier
								.setText("Build Multi-Classifier");
						mnuBuildMultiClassifier.setAccelerator(KeyStroke
								.getKeyStroke("ctrl pressed M"));
						mnuBuildMultiClassifier
								.addActionListener(new ActionListener() {
									public void actionPerformed(ActionEvent evt) {
										mnuBuildMultiClassifierActionPerformed(evt);
									}
								});
					}
				}
				{
					mnuMClassifier = new JMenu();
					mnubar.add(mnuMClassifier);
					mnuMClassifier.setText("M Classifier");
					{
						mnuMBuildClassifier = new JMenuItem();
						mnuMClassifier.add(mnuMBuildClassifier);
						mnuMBuildClassifier.setText("Build MClassifier");
						mnuMBuildClassifier
								.addActionListener(new ActionListener() {
									public void actionPerformed(ActionEvent evt) {
										mnuMBuildClassifierActionPerformed(evt);
									}
								});
					}
					{
						mnuBuildMClassifier = new JMenuItem();
						mnuMClassifier.add(mnuBuildMClassifier);
						mnuBuildMClassifier.setText("Build MClassifier2");
						mnuBuildMClassifier
								.addActionListener(new ActionListener() {
									public void actionPerformed(ActionEvent evt) {
										mnuBuildMClassifierActionPerformed(evt);
									}
								});
					}
				}
				{
					mnuMapReduce = new JMenu();
					mnubar.add(mnuMapReduce);
					mnuMapReduce.setText("MapReduce");
					{
						mnuBuildMapReduce = new JMenuItem();
						mnuMapReduce.add(mnuBuildMapReduce);
						mnuBuildMapReduce.setText("Build MR Classifier");
						mnuBuildMapReduce.addActionListener(new ActionListener() {
							public void actionPerformed(ActionEvent evt) {
								mnuBuildMapReduceActionPerformed(evt);
							}
						});
					}
				}
				{
					mnuClean = new JMenu();
					mnubar.add(mnuClean);
					mnuClean.setText("Clean");
					{
						mnuCleanItem = new JMenuItem();
						mnuClean.add(mnuCleanItem);
						mnuCleanItem.setText("Clean");
						mnuCleanItem.setAccelerator(KeyStroke
								.getKeyStroke("ctrl pressed D"));
						mnuCleanItem.addActionListener(new ActionListener() {
							public void actionPerformed(ActionEvent evt) {
								txt.setText("");
							}
						});
					}
					{
						mnuClearLocalVariables = new JMenuItem();
						mnuClean.add(mnuClearLocalVariables);
						mnuClearLocalVariables.setText("Clear Locals");
						mnuClearLocalVariables.setAccelerator(KeyStroke
								.getKeyStroke("shift pressed D"));
						mnuClearLocalVariables
								.addActionListener(new ActionListener() {
									public void actionPerformed(ActionEvent evt) {
										System.out
												.println("mnuClearLocalVariables.actionPerformed, event="
														+ evt);
										object.clear();
										map.clear();
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

	enum ChooseType {
		ARFF, OPEN, SAVE
	};

	private String choose(String currentFile) {
		if (currentFile == null)
			currentFile = "data";
		JFileChooser fileChooser = new JFileChooser(currentFile);
		int result;
		fileChooser.setFileSelectionMode(JFileChooser.FILES_ONLY);
		result = fileChooser.showOpenDialog(this);
		if (result == JFileChooser.APPROVE_OPTION) {
			String fileName = fileChooser.getSelectedFile().getAbsolutePath();
			return fileName;
		} else
			return null;
	}

	private void mnueBuildClassifierActionPerformed(ActionEvent evt) {
		double support = Double.valueOf(txtSupport.getText());
		double confidance = Double.valueOf(txtConfidence.getText());
		String train = map.get("train");
		if (train == null)
			return;

		Data data = new Data(train);
		DataMine datamine = new DataMine(data);
		Classifier classifier = new Classifier();

		Map<Long, Column> result = datamine.generateColumns(support, data
				.getLines());
		for (Column clmn : result.values()) {
			classifier.addAllCandidateRules(clmn
					.generateForConfidence(confidance));
		}

		object.put("columns", result);

		Set<Integer> trueCovered = classifier.build(data.getLines().size());

		// Set<Integer> remains=new TreeSet<Integer>(data.getLines());
		// remains.removeAll(notCovered);

		// Sccl defaultSccl=classifier.buildDefultSccl(notCovered, data);
		// classifier.addRule(defaultSccl);

		classifier.fillScclRules(data);

		object.put("classifier", classifier);

		for (ScclRule scclRule : classifier.ruleMap.values()) {
			txt.append("\n" + scclRule);
		}

		txt.append("\nNumber of rules " + classifier.ruleMap.size());
		txt.append("\n allLines.size= " + data.getLines().size()
				+ ", trueCovered=" + trueCovered.size());
	}

	private void mnuPrintClassifierActionPerformed(ActionEvent evt) {
		logger.info("Not implemented yet");
		// Classifier classifier=(Classifier) object.get("classifier");
		// if(classifier==null)return;
		// txt.append("\n"+ Tools.join(classifier.rules,"\n"));
	}

	private void mnuPrintColumnsActionPerformed(ActionEvent evt) {
		Map<BigInteger, Column> columns = (Map<BigInteger, Column>) object
				.get("columns");
		if (columns == null)
			return;
		txt.append("\nPrint Columns");
		txt.append("\n" + Tools.join(columns.entrySet(), "\n"));
	}

	private void mnuPrintCandidateRulesActionPerformed(ActionEvent evt) {
		Classifier classifier = (Classifier) object.get("classifier");
		if (classifier == null)
			return;
		txt.append("\nPrint candidate rules");
		txt.append("\n" + Tools.join(classifier.candidateRules, "\n"));
	}

	private void mnuPrintRulesActionPerformed(ActionEvent evt) {
		Classifier classifier = (Classifier) object.get("classifier");
		if (classifier == null)
			return;
		txt.append("\n\n Print rules");
		txt.append("\n" + Tools.join(classifier.ruleMap.values(), "\n"));
	}

	private void mnuPrintSelfPredictedActionPerformed(ActionEvent evt) {
		Classifier classifier = (Classifier) object.get("classifier");
		if (classifier == null)
			return;
		String train = map.get("train");
		if (train == null)
			return;
		Data data = new Data(train);
		List<Integer> predicted = data.predictOne(classifier);
		txt.append("\nPredicted col: \n" + Tools.join(predicted, "\n"));
		txt.append("\n" + data.toString(predicted));

	}

	private void mnuBuildMultiClassifierActionPerformed(ActionEvent evt) {
		double support = Double.valueOf(txtSupport.getText());
		double confidance = Double.valueOf(txtConfidence.getText());
		String train = map.get("train");
		if (train == null)
			return;

		Data data = new Data(train);
		DataMine datamine = new DataMine(data);
		Classifier classifier = new Classifier();

		Map<Long, Column> columnsMap = datamine.generateColumns(support, data
				.getLines());
		for (Column clmn : columnsMap.values()) {
			classifier.addAllCandidateRules(clmn
					.generateForConfidence(confidance));
		}

		// object.put("columns", result);

		// Set<Integer> notCovered=classifier.build(data.getLines().size());
		CheckResult result = classifier.buildMulti(data.getLines());
		classifier.fillScclRules(data);

		// Set<Integer> remains=new TreeSet<Integer>(data.getLines());
		// remains.removeAll(notCovered);

		// Sccl defaultSccl=classifier.buildDefultSccl(notCovered, data);
		// classifier.addRule(defaultSccl);

		object.put("multi-classifier", classifier);

		for (ScclRule scclRule : classifier.ruleMap.values()) {
			txt.append("\n" + scclRule);
		}

		txt.append("\nNumber of rules " + classifier.ruleMap.size());
		txt
				.append("\n allLines.size= "
						+ data.getLines().size()
						+ "\ntrueCovered="
						+ result.trueCovered.size()
						+ "\nFalseCovered="
						+ result.falseCoverd.size()
						+ "\nNotCovered="
						+ (data.getLines().size() - result.trueCovered.size() - result.falseCoverd
								.size())
						+ "\nAccuracy= "
						+ ((double) result.trueCovered.size() / data.getLines()
								.size()));
	}

	private void mnuMBuildClassifierActionPerformed(ActionEvent evt) {

		double support = Double.valueOf(txtSupport.getText());
		double confidance = Double.valueOf(txtConfidence.getText());
		int iteration = Integer.valueOf(txtIteration.getText());

		String train = map.get("train");
		if (train == null)
			return;

		Data data = new Data(train);
		DataMine datamine = new DataMine(data);
		Classifier mclassifier = (Classifier) object.get("mclassifier");
		if (mclassifier == null) {
			mclassifier = new Classifier();
			logger.info("add classifier to object map");
			object.put("mclassifier", mclassifier);
		}

		Map<Long, Column> columnsMap = datamine.generateColumns(support, data
				.getLines());
		for (Column clmn : columnsMap.values()) {
			mclassifier.addAllCandidateRules(clmn
					.generateForConfidence(confidance));
		}

		// object.put("columns", result);

		// Set<Integer> notCovered=classifier.build(data.getLines().size());
		Set<Integer> allLines = (Set<Integer>) object.get("allLines");
		if (allLines == null) {
			allLines = data.getLines();
			logger.info("add allLines to object map");
			object.put("allLines", allLines);
		}
		CheckResult result = mclassifier.buildMulti(allLines);
		mclassifier.fillScclRules(data);

		logger.info("remove all trueCovered :"
				+ allLines.removeAll(result.trueCovered));
		// Set<Integer> remains=new TreeSet<Integer>(data.getLines());
		// remains.removeAll(notCovered);

		// Sccl defaultSccl=classifier.buildDefultSccl(notCovered, data);
		// classifier.addRule(defaultSccl);

		object.put("mclassifier", mclassifier);
		// object.put("allLines", result.trueCoverd);

		for (ScclRule scclRule : mclassifier.ruleMap.values()) {
			txt.append("\n" + scclRule);
		}

		txt.append("\nNumber of rules " + mclassifier.ruleMap.size());
		txt
				.append("\n allLines.size= "
						+ data.getLines().size()
						+ "\ntrueCovered="
						+ result.trueCovered.size()
						+ "\nFalseCovered="
						+ result.falseCoverd.size()
						+ "\nNotCovered="
						+ (data.getLines().size() - result.trueCovered.size() - result.falseCoverd
								.size())
						+ "\nAccuracy= "
						+ ((double) result.trueCovered.size() / data.getLines()
								.size()));
	}

	private void mnuBuildMClassifierActionPerformed(ActionEvent evt) {
		double support = Double.valueOf(txtSupport.getText());
		double confidance = Double.valueOf(txtConfidence.getText());
		int iteration = Integer.valueOf(txtIteration.getText());

		String train = map.get("train");
		if (train == null)
			return;

		Data data = new Data(train);
		ScclRule.numOfAttributes = data.getNumberOfColumns();
		DataMine datamine = new DataMine(data);
		Classifier mclassifier = (Classifier) object.get("mclassifier");
		if (mclassifier == null || true) {// TODO: check the condition later
			mclassifier = new Classifier();
			logger.info("add classifier to object map");
			object.put("mclassifier", mclassifier);
		}

		Map<Long, Column> columnsMap = datamine.generateColumns(support, data
				.getLines());
		for (Column clmn : columnsMap.values()) {
			mclassifier.addAllCandidateRules(clmn
					.generateForConfidence(confidance));
		}

		mclassifier.buildMClassifier(data.getLines(), iteration);
		// txt.append("\nMClassifier: \n"+mclassifier);
		mclassifier.fillScclRules(data);

		// Set<Integer> remains=new TreeSet<Integer>(data.getLines());
		// remains.removeAll(notCovered);

		// Sccl defaultSccl=classifier.buildDefultSccl(notCovered, data);
		// classifier.addRule(defaultSccl);

		object.put("mclassifier", mclassifier);
		// object.put("allLines", result.trueCoverd);

		for (ScclRule scclRule : mclassifier.ruleMap.values()) {
			txt.append("\n" + scclRule);
		}

		txt.append("\nNumber of rules " + mclassifier.ruleMap.size());
		// txt.append("\n allLines.size= "+ data.getLines().size()
		// +"\ntrueCovered="+result.trueCovered.size()
		// +"\nFalseCovered="+result.falseCoverd.size()
		// +"\nNotCovered="+
		// (data.getLines().size()-result.trueCovered.size()-result.falseCoverd.size())
		// +"\nAccuracy= "+
		// ((double)result.trueCovered.size()/data.getLines().size()));
	}
	
	private void mnuBuildMapReduceActionPerformed(ActionEvent evt) {
		double support = Double.valueOf(txtSupport.getText());
		double confidance = Double.valueOf(txtConfidence.getText());
		String train = map.get("train");
		if (train == null)
			return;

		Data data = new Data(train);
		DataMine datamine = new DataMine(data);
		Classifier classifier = new Classifier();

		Map<Long, Column> result = datamine.generateColumns(support, data
				.getLines());
		for (Column clmn : result.values()) {
			classifier.addAllCandidateRules(clmn
					.generateForConfidence(confidance));
		}

		object.put("columns", result);

		Set<Integer> trueCovered = classifier.buildMapReduce(data);

		// Set<Integer> remains=new TreeSet<Integer>(data.getLines());
		// remains.removeAll(notCovered);

		// Sccl defaultSccl=classifier.buildDefultSccl(notCovered, data);
		// classifier.addRule(defaultSccl);

		classifier.fillScclRules(data);

		object.put("classifier", classifier);

		for (ScclRule scclRule : classifier.ruleMap.values()) {
			txt.append("\n" + scclRule);
		}

		txt.append("\nNumber of rules " + classifier.ruleMap.size());
		txt.append("\n allLines.size= " + data.getLines().size()
				+ ", trueCovered=" + trueCovered.size());
	}

}
