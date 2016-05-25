package edu.upenn.cis455.mapreduce;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;

import edu.upenn.cis455.mapreduce.worker.WorkerServlet;

public class MappingThread implements Runnable {
	WorkerServlet worker;
	Context context;
	String job;

	/**
	 * Constructor fo map threads
	 * @param workerServlet
	 * @param context
	 */
	public MappingThread(WorkerServlet workerServlet, Context context) {
		worker = workerServlet;
		this.context = context;
		job = worker.workerParams.get("job");
		// System.out.println("job");
	}

	/* Run Method
	 * @see java.lang.Runnable#run()
	 */
	@Override
	public void run() {
		Job jobClass = null;
		try {
			jobClass = (Job) Class.forName(job).newInstance();
		} catch (InstantiationException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (IllegalAccessException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (ClassNotFoundException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		while (!worker.isFileQueueEmpty()) {
			File file = worker.getFile();
			BufferedReader reader;
			try {
				reader = new BufferedReader(new FileReader(file));
				String line = "";
				while ((line = reader.readLine()) != null) {
					// System.out.println("line:  " + line);

					worker.addLine(line);
					worker.updateKeysRead();
				}
				reader.close();
			} catch (FileNotFoundException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			} catch (IOException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}

		}

		while (!worker.isLineQueueEmpty()) {
			String line = worker.getLine();
			String[] keyValuePair = line.split("\t");
			// System.out.println("key:  " + keyValuePair[0]);
			jobClass.map(keyValuePair[0], keyValuePair[1], context);
			worker.updateKeysWritten();
		}

	}

}
