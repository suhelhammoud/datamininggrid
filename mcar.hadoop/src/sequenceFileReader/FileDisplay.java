package sequenceFileReader;

import java.awt.event.*;
import java.awt.*;

import javax.swing.*;
import javax.swing.border.EtchedBorder;

/**
 * Based on code from Sun's DragFileDemo:
 * 
 * http://java.sun.com/docs/books/tutorial/uiswing/examples/dnd/index.html
 */
public class FileDisplay extends JPanel implements ActionListener {
	static String defaultdir = ".";
	private static final long serialVersionUID = -2073915390547402450L;
	private JFileChooser fileChooser;
	private JButton clearAllButton;
	private TabbedPaneController paneController;

	public FileDisplay() {
		super(new BorderLayout());

		fileChooser = new JFileChooser(defaultdir);
		fileChooser.setMultiSelectionEnabled(true);
		fileChooser.setDragEnabled(true);
		fileChooser.setControlButtonsAreShown(false);
		JPanel fcPanel = new JPanel(new BorderLayout());
		fcPanel.add(fileChooser, BorderLayout.CENTER);

		// button for clearing all file display tabs
		clearAllButton = new JButton("Clear All");
		clearAllButton.addActionListener(this);
		JPanel optionsPanel = new JPanel(new BorderLayout());
		optionsPanel.setBorder(BorderFactory.createEmptyBorder(5, 5, 5, 5));
		optionsPanel.add(clearAllButton, BorderLayout.LINE_END);
		// panel with text entry for entering limit of lines to display for
		// sequence files
		JPanel lineLimitPanel = new JPanel(new FlowLayout());
		JLabel topLabel = new JLabel("Line limit:");
		lineLimitPanel.add(topLabel);
		JTextArea limitEntry = new JTextArea(1, 5);
		limitEntry.setBorder(BorderFactory
				.createEtchedBorder(EtchedBorder.LOWERED));
		lineLimitPanel.add(limitEntry);
		optionsPanel.add(lineLimitPanel, BorderLayout.LINE_START);

		JPanel upperPanel = new JPanel(new BorderLayout());
		upperPanel.setBorder(BorderFactory.createEmptyBorder(5, 5, 5, 5));
		upperPanel.add(fcPanel, BorderLayout.CENTER);
		upperPanel.add(optionsPanel, BorderLayout.PAGE_END);

		// The TabbedPaneController manages the panel that
		// contains the tabbed pane. When there are no files
		// the panel contains a plain text area. Then, as
		// files are dropped onto the area, the tabbed panel
		// replaces the file area.
		JTabbedPane tabbedPane = new JTabbedPane();
		JPanel tabPanel = new JPanel(new BorderLayout());
		tabPanel.setBorder(BorderFactory.createEmptyBorder(5, 5, 5, 5));
		paneController = new TabbedPaneController(tabbedPane, tabPanel,
				limitEntry);

		JSplitPane splitPane = new JSplitPane(JSplitPane.VERTICAL_SPLIT,
				upperPanel, tabPanel);
		splitPane.setDividerLocation(400);
		splitPane.setPreferredSize(new Dimension(530, 650));
		add(splitPane, BorderLayout.CENTER);
	}

	public void setDefaultButton() {
		getRootPane().setDefaultButton(clearAllButton);
	}

	public void actionPerformed(ActionEvent event) {
		if (event.getSource() == clearAllButton) {
			paneController.clearAll();
		}
	}

	/**
	 * Create the GUI and show it. For thread safety, this method should be
	 * invoked from the event-dispatching thread.
	 */
	private static void createAndShowGUI() {
		// Create and set up the window.
		JFrame frame = new JFrame("(Sequence)FileDisplay");
		frame.setDefaultCloseOperation(JFrame.EXIT_ON_CLOSE);

		// Create and set up the menu bar and content pane.
		FileDisplay fileDisplay = new FileDisplay();
		fileDisplay.setOpaque(true); // content panes must be opaque
		frame.setContentPane(fileDisplay);

		// Display the window.
		frame.pack();
		frame.setVisible(true);
		fileDisplay.setDefaultButton();
	}

	public static void main(String[] args) {
		// Schedule a job for the event-dispatching thread:
		// creating and showing this application's GUI.
		javax.swing.SwingUtilities.invokeLater(new Runnable() {
			public void run() {
				createAndShowGUI();
			}
		});
	}
}
