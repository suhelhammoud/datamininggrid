package gui;

import javax.swing.JFileChooser;
import javax.swing.JOptionPane;
import java.awt.Frame;

import java.io.*;

public class Chooser {
	private String fileName;
	private boolean isSelected = false;
	private Frame frame;

	public Chooser(Frame PFrame) {
		frame = PFrame;
	}

	public boolean isFileSelected(String dialogType) {
		JFileChooser chooser = new JFileChooser();
		chooser.setCurrentDirectory(new File("data/1.arff"));
		if (dialogType.equals("open"))
			chooser.setDialogType(JFileChooser.OPEN_DIALOG);
		else if (dialogType.equals("save"))
			chooser.setDialogType(JFileChooser.SAVE_DIALOG);
		else {
			chooser.setDialogType(JFileChooser.CUSTOM_DIALOG);
			chooser.setApproveButtonText(dialogType);
		}
		String result = "";
		int retval = chooser.showDialog(frame, null);
		if (retval == JFileChooser.APPROVE_OPTION) {
			result = chooser.getSelectedFile().getAbsolutePath();
			fileName = result;
			isSelected = true;
			return true;
		}
		isSelected = false;
		return false;
	}

	public String getFileName() {
		if (isSelected)
			return fileName;
		else
			JOptionPane.showMessageDialog(null, "error\nno file is choosen");
		return null;
	}
}