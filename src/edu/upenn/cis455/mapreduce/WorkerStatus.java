package edu.upenn.cis455.mapreduce;

import java.util.Calendar;
import java.util.Date;

public class WorkerStatus {
	String IPAddress;
	int port;
	String status;
	String job;
	int keysRead;
	int keysWritten;
	Long lastAccessed;

	public WorkerStatus(String IPAddress, int port, String status, String job,
			int keysRead, int keysWritten) {
		this.IPAddress = IPAddress;
		this.port = port;
		this.status = status;
		this.job = job;
		this.keysRead = keysRead;
		this.keysWritten = keysWritten;
		this.lastAccessed = System.currentTimeMillis();
	}

	/**
	 * @return the iPAddress
	 */
	public String getIPAddress() {
		return IPAddress;
	}

	/**
	 * @param iPAddress
	 *            the iPAddress to set
	 */
	public void setIPAddress(String iPAddress) {
		IPAddress = iPAddress;
	}

	/**
	 * @return the port
	 */
	public int getPort() {
		return port;
	}

	/**
	 * @param port
	 *            the port to set
	 */
	public void setPort(int port) {
		this.port = port;
	}

	/**
	 * @return the status
	 */
	public String getStatus() {
		return status;
	}

	/**
	 * @param status
	 *            the status to set
	 */
	public void setStatus(String status) {
		this.status = status;
	}

	/**
	 * @return the job
	 */
	public String getJob() {
		return job;
	}

	/**
	 * @param job
	 *            the job to set
	 */
	public void setJob(String job) {
		this.job = job;
	}

	/**
	 * @return the keysRead
	 */
	public int getKeysRead() {
		return keysRead;
	}

	/**
	 * @param keysRead
	 *            the keysRead to set
	 */
	public void setKeysRead(int keysRead) {
		this.keysRead = keysRead;
	}

	/**
	 * @return the keysWritten
	 */
	public int getKeysWritten() {
		return keysWritten;
	}

	/**
	 * @param keysWritten
	 *            the keysWritten to set
	 */
	public void setKeysWritten(int keysWritten) {
		this.keysWritten = keysWritten;
	}

	/**
	 * @return the lastAccessed
	 */
	public Long getLastAccessed() {
		return lastAccessed;
	}

	/**
	 * @param lastAccessed
	 *            the lastAccessed to set
	 */
	public void setLastAccessed(Long lastAccessed) {
		this.lastAccessed = lastAccessed;
	}

}
