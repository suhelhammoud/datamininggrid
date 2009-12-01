package hasim;

import org.apache.log4j.Logger;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

import eduni.simjava.Sim_event;
import eduni.simjava.Sim_port;
import eduni.simjava.Sim_system;
import eduni.simjava.distributions.ContinuousGenerator;
import gridsim.GridSim;
import gridsim.GridSimTags;
import gridsim.ParameterException;
import gridsim.datagrid.File;

public class HDisk extends GridSim {
	/**
	 * Logger for this class
	 */
	private static final Logger logger = Logger.getLogger(HDisk.class);

	Sim_port in;

	@Override
	public void body() {

		while (Sim_system.running()) {
			Sim_event ev = new Sim_event();

			super.sim_get_next(ev);

			int tag = ev.get_tag();
			logger.info("tag  " + HTAG.get(tag) + " from "
					+ super.getEntityName(ev.get_src()) + " data :"
					+ ev.get_data() + " at time " + GridSim.clock());

			// if the simulation finishes then exit the loop
			if (tag == GridSimTags.END_OF_SIMULATION) {
				logger.info("receive end of simulation event");
				break;
			}

			//TODO to delete later
			if (tag < HTAG.HDBASE)
				continue;

			Datum file = (Datum) ev.get_data();
			if (file == null) {
				logger.error("null file on clock " + GridSim.clock());
			}

			// TODO add more accurate formulaa
			if (tag == HTAG.readDelta.id()) {
				double sz = file.delta;
				sim_process((double) sz / maxTransferRateRead);
				send(ev.get_src(), 0.0, tag, file);
				continue;
			}

			// TODO add more accurate formula
			if (tag == HTAG.writeDelta.id()) {
				double sz = file.delta;
				sim_process((double) sz / maxTransferRateWrite);
				send(ev.get_src(), 0.0, tag, file);
				continue;
			}

			// process the received event
		}

		// remove I/O entities created during construction of this entity
		// super.terminateIOEntities();
	}

	// public HardDrive(String name) throws Exception {
	// this.name = name;
	// }
	//
	// public void setName(String name) {
	// this.name = name;
	// }

	// private long capacity;
	// private int seekTime = 2;
	// private int writeSpeed = 30000;
	// private int readSpeed = 60000;

	private long used = 0;

	/** a list storing the names of all the files on the harddrive */
	private ArrayList nameList_;

	/** a list storing all the files stored on the harddrive */
	private ArrayList fileList_;

	/** the name of the harddrive */
	private String name;

	/** a generator required to randomize the seek time */
	private ContinuousGenerator gen_;

	/** the current size of files on the harddrive */
	private double currentSize_;

	/** the total capacity of the harddrive in MB */
	private double capacity_;

	/** the maximum transfer rate in MB/sec */
	private double maxTransferRateRead;

	/** added by suhel in MB/sec */
	private double maxTransferRateWrite;

	/** the latency of the harddrive in seconds */
	private double latency_;

	/** the average seek time in seconds */
	private double avgSeekTime_;

	/**
	 * Creates a new harddrive storage with a given name and capacity.
	 * 
	 * @param name
	 *            the name of the new harddrive storage
	 * @param capacity
	 *            the capacity in MByte
	 * @throws Exception
	 */
	public HDisk(String name, double capacity) throws Exception {
		super(name);
		if (name == null || name.length() == 0) {
			throw new ParameterException(
					"HarddriveStorage(): Error - invalid storage name.");
		}

		if (capacity <= 0) {
			throw new ParameterException(
					"HarddriveStorage(): Error - capacity <= 0.");
		}

		name = name;
		capacity_ = capacity;
		in = new Sim_port("in");
		add_port(in);
		init();
	}

	public void init() {
		fileList_ = new ArrayList();
		nameList_ = new ArrayList();
		gen_ = null;
		currentSize_ = 0;

		latency_ = 1; // 4.17 ms in seconds
		avgSeekTime_ = 2; // 9 ms
		maxTransferRateRead = 10; // in MB/sec
		maxTransferRateWrite = 5;
	}

	/**
	 * The initialization of the harddrive is done in this method. The most
	 * common parameters, such as latency, average seek time and maximum
	 * transfer rate are set. The default values are set to simulate the Maxtor
	 * DiamonMax 10 ATA harddisk. Furthermore, the necessary lists are created.
	 */
	private void init_old() {
		fileList_ = new ArrayList();
		nameList_ = new ArrayList();
		gen_ = null;
		currentSize_ = 0;

		latency_ = 0.00417; // 4.17 ms in seconds
		avgSeekTime_ = 0.009; // 9 ms
		maxTransferRateRead = 133; // in MB/sec
	}

	/**
	 * Gets the available space on this storage in MB.
	 * 
	 * @return the available space in MB
	 */
	public double getAvailableSpace() {
		return capacity_ - currentSize_;
	}

	/**
	 * Checks if the storage is full or not.
	 * 
	 * @return <tt>true</tt> if the storage is full, <tt>false</tt> otherwise
	 */
	public boolean isFull() {
		if (currentSize_ == capacity_) {
			return true;
		}
		return false;
	}

	/**
	 * Gets the number of files stored on this storage.
	 * 
	 * @return the number of stored files
	 */
	public int getNumStoredFile() {
		return fileList_.size();
	}

	/**
	 * Makes a reservation of the space on the storage to store a file.
	 * 
	 * @param fileSize
	 *            the size to be reserved in MB
	 * @return <tt>true</tt> if reservation succeeded, <tt>false</tt> otherwise
	 */
	public boolean reserveSpace(int fileSize) {
		if (fileSize <= 0) {
			return false;
		}

		if (currentSize_ + fileSize >= capacity_) {
			return false;
		}

		currentSize_ += fileSize;
		return true;
	}

	/**
	 * Adds a file for which the space has already been reserved. The time taken
	 * (in seconds) for adding the file can also be found using
	 * {@link gridsim.datagrid.File#getTransactionTime()}.
	 * 
	 * @param file
	 *            the file to be added
	 * @return the time (in seconds) required to add the file
	 */
	public double addReservedFile(File file) {
		if (file == null) {
			return 0;
		}

		currentSize_ -= file.getSize();
		double result = addFile(file);

		// if add file fails, then set the current size back to its old value
		if (result == 0.0) {
			currentSize_ += file.getSize();
		}

		return result;
	}

	/**
	 * Checks whether there is enough space on the storage for a certain file.
	 * 
	 * @param fileSize
	 *            a FileAttribute object to compare to
	 * @return <tt>true</tt> if enough space available, <tt>false</tt> otherwise
	 */
	public boolean hasPotentialAvailableSpace(int fileSize) {
		if (fileSize <= 0) {
			return false;
		}

		// check if enough space left
		if (getAvailableSpace() > fileSize) {
			return true;
		}

		Iterator it = fileList_.iterator();
		File file = null;
		int deletedFileSize = 0;

		// if not enough space, then if want to clear/delete some files
		// then check whether it still have space or not
		boolean result = false;
		while (it.hasNext()) {
			file = (File) it.next();
			if (!file.isReadOnly()) {
				deletedFileSize += file.getSize();
			}

			if (deletedFileSize > fileSize) {
				result = true;
				break;
			}
		}

		return result;
	}

	/**
	 * Gets the total capacity of the storage in MB.
	 * 
	 * @return the capacity of the storage in MB
	 */
	public double getCapacity() {
		return capacity_;
	}

	/**
	 * Gets the current size of the stored files in MB.
	 * 
	 * @return the current size of the stored files in MB
	 */
	public double getCurrentSize() {
		return currentSize_;
	}

	/**
	 * Sets the latency of this harddrive in seconds.
	 * 
	 * @param latency
	 *            the new latency in seconds
	 * @return <tt>true</tt> if the setting succeeded, <tt>false</tt> otherwise
	 */
	public boolean setLatency(double latency) {
		if (latency < 0) {
			return false;
		}

		latency_ = latency;
		return true;
	}

	/**
	 * Gets the latency of this harddrive in seconds.
	 * 
	 * @return the latency in seconds
	 */
	public double getLatency() {
		return latency_;
	}

	/**
	 * Sets the maximum transfer rate of this storage system in MB/sec.
	 * 
	 * @param rate
	 *            the maximum transfer rate in MB/sec
	 * @return <tt>true</tt> if the setting succeeded, <tt>false</tt> otherwise
	 */
	public boolean setMaxTransferRate(int rate) {
		if (rate <= 0) {
			return false;
		}

		maxTransferRateRead = rate;
		return true;
	}

	/**
	 * Gets the maximum transfer rate of the storage in MB/sec.
	 * 
	 * @return the maximum transfer rate in MB/sec
	 */
	public double getMaxTransferRate() {
		return maxTransferRateRead;
	}

	/**
	 * Sets the average seek time of the storage in seconds.
	 * 
	 * @param seekTime
	 *            the average seek time in seconds
	 * @return <tt>true</tt> if the setting succeeded, <tt>false</tt> otherwise
	 */
	public boolean setAvgSeekTime(double seekTime) {
		return setAvgSeekTime(seekTime, null);
	}

	/**
	 * Sets the average seek time and a new generator of seek times in seconds.
	 * The generator determines a randomized seek time.
	 * 
	 * @param seekTime
	 *            the average seek time in seconds
	 * @param gen
	 *            the ContinuousGenerator which generates seek times
	 * @return <tt>true</tt> if the setting succeeded, <tt>false</tt> otherwise
	 */
	public boolean setAvgSeekTime(double seekTime, ContinuousGenerator gen) {
		if (seekTime <= 0.0) {
			return false;
		}

		avgSeekTime_ = seekTime;
		gen_ = gen;
		return true;
	}

	/**
	 * Gets the average seek time of the harddrive in seconds.
	 * 
	 * @return the average seek time in seconds
	 */
	public double getAvgSeekTime() {
		return avgSeekTime_;
	}

	/**
	 * Gets the file with the specified name. The time taken (in seconds) for
	 * getting the file can also be found using
	 * {@link gridsim.datagrid.File#getTransactionTime()}.
	 * 
	 * @param fileName
	 *            the name of the needed file
	 * @return the file with the specified filename
	 */
	public File getFile(String fileName) {
		// check first whether file name is valid or not
		File obj = null;
		if (fileName == null || fileName.length() == 0) {
			System.out.println(name + ".getFile(): Warning - invalid "
					+ "file name.");
			return obj;
		}

		Iterator it = fileList_.iterator();
		int size = 0;
		int index = 0;
		boolean found = false;
		File tempFile = null;

		// find the file in the disk
		while (it.hasNext()) {
			tempFile = (File) it.next();
			size += tempFile.getSize();
			if (tempFile.getName().equals(fileName)) {
				found = true;
				obj = tempFile;
				break;
			}

			index++;
		}

		// if the file is found, then determine the time taken to get it
		if (found) {
			obj = (File) fileList_.get(index);
			double seekTime = getSeekTime(size);
			double transferTime = getTransferTime(obj.getSize());

			// total time for this operation
			obj.setTransactionTime(seekTime + transferTime);
		}

		return obj;
	}

	/**
	 * Gets the list of file names located on this storage.
	 * 
	 * @return a LinkedList of file names
	 */
	public List getFileNameList() {
		return nameList_;
	}

	/**
	 * Get the seek time for a file with the defined size. Given a file size in
	 * MB, this method returns a seek time for the file in seconds.
	 * 
	 * @param fileSize
	 *            the size of a file in MB
	 * @return the seek time in seconds
	 */
	private double getSeekTime(int fileSize) {
		double result = 0;

		if (gen_ != null) {
			result += gen_.sample();
		}

		if (fileSize > 0 && capacity_ != 0) {
			result += ((double) fileSize / capacity_);
		}

		return result;
	}

	/**
	 * Gets the transfer time of a given file
	 * 
	 * @param fileSize
	 *            the size of the transferred file
	 * @return the transfer time in seconds
	 */
	private double getTransferTime(int fileSize) {
		double result = 0;
		if (fileSize > 0 && capacity_ != 0) {
			result = ((double) fileSize * maxTransferRateRead) / capacity_;
		}

		return result;
	}

	/**
	 * Check if the file is valid or not. This method checks whether the given
	 * file or the file name of the file is valid. The method name parameter is
	 * used for debugging purposes, to output in which method an error has
	 * occured.
	 * 
	 * @param file
	 *            the file to be checked for validity
	 * @param methodName
	 *            the name of the method in which we check for validity of the
	 *            file
	 * @return <tt>true</tt> if the file is valid, <tt>false</tt> otherwise
	 */
	private boolean isFileValid(File file, String methodName) {

		if (file == null) {
			System.out.println(name + "." + methodName
					+ ": Warning - the given file is null.");
			return false;
		}

		String fileName = file.getName();
		if (fileName == null || fileName.length() == 0) {
			System.out.println(name + "." + methodName
					+ ": Warning - invalid file name.");
			return false;
		}

		return true;
	}

	/**
	 * Adds a file to the storage. First, the method checks if there is enough
	 * space on the storage, then it checks if the file with the same name is
	 * already taken to avoid duplicate filenames. <br>
	 * The time taken (in seconds) for adding the file can also be found using
	 * {@link gridsim.datagrid.File#getTransactionTime()}.
	 * 
	 * @param file
	 *            the file to be added
	 * @return the time taken (in seconds) for adding the specified file
	 */
	public double addFile(File file) {
		double result = 0.0;
		// check if the file is valid or not
		if (!isFileValid(file, "addFile()")) {
			return result;
		}

		// check the capacity
		if (file.getSize() + currentSize_ > capacity_) {
			System.out.println(name + ".addFile(): Warning - not enough space"
					+ " to store " + file.getName());
			return result;
		}

		// check if the same file name is alredy taken
		if (!contains(file.getName())) {
			double seekTime = getSeekTime(file.getSize());
			double transferTime = getTransferTime(file.getSize());

			fileList_.add(file); // add the file into the HD
			nameList_.add(file.getName()); // add the name to the name list
			currentSize_ += file.getSize(); // increment the current HD size
			result = seekTime + transferTime; // add total time
		}
		file.setTransactionTime(result);
		return result;
	}

	/**
	 * Adds a set of files to the storage. Runs through the list of files and
	 * save all of them. The time taken (in seconds) for adding each file can
	 * also be found using {@link gridsim.datagrid.File#getTransactionTime()}.
	 * 
	 * @param list
	 *            the files to be added
	 * @return the time taken (in seconds) for adding the specified files
	 */
	public double addFile(List list) {
		double result = 0.0;
		if (list == null || list.size() == 0) {
			System.out.println(name + ".addFile(): Warning - list is empty.");
			return result;
		}

		Iterator it = list.iterator();
		File file = null;
		while (it.hasNext()) {
			file = (File) it.next();
			result += addFile(file); // add each file in the list
		}
		return result;
	}

	/**
	 * Removes a file from the storage. The time taken (in seconds) for deleting
	 * the file can also be found using
	 * {@link gridsim.datagrid.File#getTransactionTime()}.
	 * 
	 * @param fileName
	 *            the name of the file to be removed
	 * @return the deleted file
	 */
	public File deleteFile(String fileName) {
		if (fileName == null || fileName.length() == 0) {
			return null;
		}

		Iterator it = fileList_.iterator();
		File file = null;
		while (it.hasNext()) {
			file = (File) it.next();
			String name = file.getName();

			// if a file is found then delete
			if (fileName.equals(name)) {
				double result = deleteFile(file);
				file.setTransactionTime(result);
				break;
			} else {
				file = null;
			}
		}
		return file;
	}

	/**
	 * Removes a file from the storage. The time taken (in seconds) for deleting
	 * the file can also be found using
	 * {@link gridsim.datagrid.File#getTransactionTime()}.
	 * 
	 * @param fileName
	 *            the name of the file to be removed
	 * @param file
	 *            the file which is removed from the storage is returned through
	 *            this parameter
	 * @return the time taken (in seconds) for deleting the specified file
	 */
	public double deleteFile(String fileName, File file) {
		return deleteFile(file);
	}

	/**
	 * Removes a file from the storage. The time taken (in seconds) for deleting
	 * the file can also be found using
	 * {@link gridsim.datagrid.File#getTransactionTime()}.
	 * 
	 * @param file
	 *            the file which is removed from the storage is returned through
	 *            this parameter
	 * @return the time taken (in seconds) for deleting the specified file
	 */
	public double deleteFile(File file) {
		double result = 0.0;
		// check if the file is valid or not
		if (!isFileValid(file, "deleteFile()")) {
			return result;
		}
		double seekTime = getSeekTime(file.getSize());
		double transferTime = getTransferTime(file.getSize());

		// check if the file is in the storage
		if (contains(file)) {
			fileList_.remove(file); // remove the file HD
			nameList_.remove(file.getName()); // remove the name from name list
			currentSize_ -= file.getSize(); // decrement the current HD space
			result = seekTime + transferTime; // total time
			file.setTransactionTime(result);
		}
		return result;
	}

	/**
	 * Checks whether a certain file is on the storage or not.
	 * 
	 * @param fileName
	 *            the name of the file we are looking for
	 * @return <tt>true</tt> if the file is in the storage, <tt>false</tt>
	 *         otherwise
	 */
	public boolean contains(String fileName) {
		boolean result = false;
		if (fileName == null || fileName.length() == 0) {
			System.out.println(name
					+ ".contains(): Warning - invalid file name");
			return result;
		}
		// check each file in the list
		Iterator it = nameList_.iterator();
		while (it.hasNext()) {
			String name = (String) it.next();
			if (name.equals(fileName)) {
				result = true;
				break;
			}
		}
		return result;
	}

	/**
	 * Checks whether a certain file is on the storage or not.
	 * 
	 * @param file
	 *            the file we are looking for
	 * @return <tt>true</tt> if the file is in the storage, <tt>false</tt>
	 *         otherwise
	 */
	public boolean contains(File file) {
		boolean result = false;
		if (!isFileValid(file, "contains()")) {
			return result;
		}

		result = contains(file.getName());
		return result;
	}

	/**
	 * Renames a file on the storage. The time taken (in seconds) for renaming
	 * the file can also be found using
	 * {@link gridsim.datagrid.File#getTransactionTime()}.
	 * 
	 * @param file
	 *            the file we would like to rename
	 * @param newName
	 *            the new name of the file
	 * 
	 * @return <tt>true</tt> if the renaming succeeded, <tt>false</tt> otherwise
	 */
	public boolean renameFile(File file, String newName) {
		// check whether the new filename is conflicting with existing ones
		// or not
		boolean result = false;
		if (contains(newName)) {
			return result;
		}

		// replace the file name in the file (physical) list
		File obj = getFile(file.getName());
		if (obj != null) {
			obj.setName(newName);
		} else {
			return result;
		}

		// replace the file name in the name list
		Iterator it = nameList_.iterator();
		while (it.hasNext()) {
			String name = (String) it.next();
			if (name.equals(file.getName())) {
				file.setTransactionTime(0);
				nameList_.remove(name);
				nameList_.add(newName);
				result = true;
				break;
			}
		}

		return result;
	}

	public static void main(String[] args) {
		System.out.println(Integer.MAX_VALUE / 1000000000);
	}

}
