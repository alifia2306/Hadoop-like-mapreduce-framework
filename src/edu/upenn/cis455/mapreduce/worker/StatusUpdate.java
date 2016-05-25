package edu.upenn.cis455.mapreduce.worker;

import edu.upenn.cis455.mapreduce.Client;

public class StatusUpdate extends Thread {
	WorkerServlet worker;
	Client client;
	boolean flag = true;

	/**
	 * Construtor for status update
	 * @param worker
	 */
	public StatusUpdate(WorkerServlet worker) {
		this.worker = worker;
		client = new Client();
	}

	/**
	 * Method to set flag for status notifications
	 * @param f
	 */
	public void setFlag(boolean f) {
		flag = f;
	}

	/* 
	 * Run method to run thread of status update
	 * (non-Javadoc)
	 * @see java.lang.Thread#run()
	 */
	public void run() {

		while (flag) {

			client.clearMaps();
			client.setUrl("http://" + worker.IPPortMaster
					+ "/master/workerstatus");
			client.setParameters("port", "" + worker.port);
			client.setParameters("status", worker.workerParams.get("status"));
			client.setParameters("job", worker.workerParams.get("job"));
			client.setParameters("keysRead", "" + worker.keysRead);
			client.setParameters("keysWritten", "" + worker.keysWritten);
			client.sendGETRequest();
			try {
				Thread.sleep(10000);
			} catch (InterruptedException e) {
				continue;
			}
		}
	}

}
