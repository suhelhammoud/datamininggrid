package sequenceFileReader;

/*
 * FileAndTextTransferHandler.java is used by the 1.4
 * DragFileDemo.java example.
 */

import java.awt.datatransfer.DataFlavor;
import java.awt.datatransfer.StringSelection;
import java.awt.datatransfer.Transferable;
import java.awt.datatransfer.UnsupportedFlavorException;
import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import javax.swing.JComponent;
import javax.swing.JTextArea;
import javax.swing.TransferHandler;
import javax.swing.text.BadLocationException;
import javax.swing.text.Document;
import javax.swing.text.JTextComponent;
import javax.swing.text.Position;

import org.apache.commons.io.FileUtils;
import org.apache.hadoop.fs.Path;

/**
 * Based on code from Sun's DragFileDemo:
 * 
 * http://java.sun.com/docs/books/tutorial/uiswing/examples/dnd/index.html
 */
public class FileAndTextTransferHandler extends TransferHandler {

	private static final long serialVersionUID = -3875297156720880551L;
	private DataFlavor fileFlavor, stringFlavor;
	private TabbedPaneController paneController;
	private JTextArea source;
	private boolean shouldRemove;
	protected String newline = "\n";
	private final static String FILE_PROTOCOL = "file://";

	// Start and end position in the source text.
	// We need this information when performing a MOVE
	// in order to remove the dragged text from the source.
	private Position pos0 = null, pos1 = null;

	FileAndTextTransferHandler(TabbedPaneController controller) {
		paneController = controller;
		fileFlavor = DataFlavor.javaFileListFlavor;
		stringFlavor = DataFlavor.stringFlavor;
	}

	/**
	 * Add the contents of each file to a tab.
	 * 
	 * @param files
	 *            List of files to be displayed.
	 */
	private void displayFiles(List<File> files) {
		for (File file : files) {
			// Tell the tabbedpane controller to add
			// a new tab with the name of this file
			// on the tab. The text area that will
			// display the contents of the file is returned.
			JTextArea textArea = paneController.addTab(file.toString());

			try { // first try process it as a sequence file
				Path sequenceFilePath = new Path(file.getAbsolutePath());
				String sequenceFileContents = SequenceFileUtils
						.readSequenceFileTop(sequenceFilePath, paneController
								.getLineLimit());
				textArea.append(sequenceFileContents + newline);
			} catch (IOException e) {
				// if we get here then it's probably not a sequence file
				System.err
						.println("Error processing as sequence file, attempting to convert to a string ("
								+ e.getMessage() + ")");
				try {
					// if we read in as plain text we ignore line limit
					textArea.append(FileUtils.readFileToString(file));
				} catch (IOException e1) {
					e.printStackTrace();
				}
			} catch (Exception e) {
				e.printStackTrace();
			}
		}
	}

	@SuppressWarnings("unchecked")
	public boolean importData(JComponent component, Transferable transferable) {
		if (!canImport(component, transferable.getTransferDataFlavors())) {
			return false;
		}
		// A real application would load the file in another
		// thread in order to not block the UI. This step
		// was omitted here to simplify the code.
		try {
			if (hasFileFlavor(transferable.getTransferDataFlavors())) {
				List<File> files = (List<File>) transferable
						.getTransferData(fileFlavor);
				displayFiles(files);
				return true;
			} else if (hasStringFlavor(transferable.getTransferDataFlavors())) {
				JTextArea textArea = (JTextArea) component;
				if (textArea.equals(source)
						&& (textArea.getCaretPosition() >= pos0.getOffset())
						&& (textArea.getCaretPosition() <= pos1.getOffset())) {
					shouldRemove = false;
					return true;
				}
				String rawTransferData = (String) transferable
						.getTransferData(stringFlavor);
				if (rawTransferData.startsWith(FILE_PROTOCOL)) {
					// string(s) representing files, open the files themselves
					List files = extractFiles(rawTransferData);
					displayFiles(files);
				} else { // not a file:/// url, just display raw text
					textArea.replaceSelection(rawTransferData);
				}
				return true;
			}
		} catch (UnsupportedFlavorException e) {
			e.printStackTrace();
		} catch (IOException e) {
			e.printStackTrace();
		}
		return false;
	}

	/**
	 * Extracts all the files from the passed input string which contains one or
	 * more files represented by url's of the form file:///path.
	 * 
	 * @param inputString
	 *            Input containing file url(s).
	 * @return A list of Files represented by the passed input string.
	 */
	private List<File> extractFiles(String inputString) {
		String[] fileParts = inputString.split("\\s+");
		List<File> droppedFiles = new ArrayList<File>();
		for (String filePart : fileParts) {
			if (!filePart.startsWith(FILE_PROTOCOL)) {
				System.err.println("Cannot process " + filePart
						+ ", doesn't appear to be a file");
			} else {
				filePart = filePart.substring(FILE_PROTOCOL.length());
				File droppedFile = new File(filePart);
				droppedFiles.add(droppedFile);
			}
		}
		return droppedFiles;
	}

	protected Transferable createTransferable(JComponent component) {
		source = (JTextArea) component;
		int start = source.getSelectionStart();
		int end = source.getSelectionEnd();
		Document doc = source.getDocument();
		if (start == end) {
			return null;
		}
		try {
			pos0 = doc.createPosition(start);
			pos1 = doc.createPosition(end);
		} catch (BadLocationException e) {
			e.printStackTrace();
		}
		shouldRemove = true;
		String data = source.getSelectedText();
		return new StringSelection(data);
	}

	public int getSourceActions(JComponent c) {
		return COPY_OR_MOVE;
	}

	// Remove the old text if the action is a MOVE.
	// However, we do not allow dropping on top of the selected text,
	// so in that case do nothing.
	protected void exportDone(JComponent c, Transferable data, int action) {
		if (shouldRemove && (action == MOVE)) {
			if ((pos0 != null) && (pos1 != null)
					&& (pos0.getOffset() != pos1.getOffset())) {
				try {
					JTextComponent tc = (JTextComponent) c;
					tc.getDocument().remove(pos0.getOffset(),
							pos1.getOffset() - pos0.getOffset());
				} catch (BadLocationException e) {
					e.printStackTrace();
				}
			}
		}
		source = null;
	}

	public boolean canImport(JComponent c, DataFlavor[] flavors) {
		if (hasFileFlavor(flavors)) {
			return true;
		}
		if (hasStringFlavor(flavors)) {
			return true;
		}
		return false;
	}

	private boolean hasFileFlavor(DataFlavor[] flavors) {
		for (int i = 0; i < flavors.length; i++) {
			if (fileFlavor.equals(flavors[i])) {
				return true;
			}
		}
		return false;
	}

	private boolean hasStringFlavor(DataFlavor[] flavors) {
		for (int i = 0; i < flavors.length; i++) {
			if (stringFlavor.equals(flavors[i])) {
				return true;
			}
		}
		return false;
	}
}
