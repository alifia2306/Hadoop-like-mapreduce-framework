package edu.upenn.cis455.mapreduce;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.util.ArrayList;

import edu.upenn.cis455.mapreduce.worker.WorkerServlet;

public class ReducingThread implements Runnable {

	private Job job;
	private Context context;
	private BufferedReader bufferedReader;
	private String word;
	private String nextWord;
	private ArrayList<String> occurenceList = new ArrayList<String>();
	private WorkerServlet workerServlet;
	String line = "";

	/**
	 * Construtor for reducing thread
	 * @param worker
	 * @param context
	 * @param spoolIN
	 */
	public ReducingThread(WorkerServlet worker, Context context, String spoolIN) {
		workerServlet = worker;
		try {
			job = (Job) Class.forName(worker.workerParams.get("job"))
					.newInstance();
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

		this.context = context;
		word = null;
		nextWord = null;
		File sortedFile = new File(spoolIN + "/sorted.txt");
		// System.out.println("Sorted file:  " + worker.spoolInDir +
		// "/sorted.txt");
		try {
			bufferedReader = new BufferedReader(new FileReader(sortedFile));
		} catch (FileNotFoundException e) {
			e.printStackTrace();
		}
	}

	/**
	 * Method to read lines of sorted file and invoke reduce
	 * @return
	 */
	private synchronized boolean hasNextLine() {
		if (nextWord == null) {
			try {
				line = bufferedReader.readLine();
			} catch (IOException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
			if (line == null || line.trim().equals("")) {
				return false;
			} else {
				// System.out.println("starting");
				workerServlet.updateKeysRead();
				String[] keyValue = line.split("\t");
				word = keyValue[0];

				// System.out.println("current word:  " + word);
				occurenceList.add(keyValue[1]);
			}

		} else {
			word = nextWord;
		}
		while (true) {
			// System.out.println("word:  " + word);
			// System.out.println("nextWord:  " + nextWord);

			try {
				line = bufferedReader.readLine();
				// System.out.println("line:  " + line);

			} catch (IOException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
			if (line == null || line.trim().equals("")) {
				nextWord = null;
				int size = occurenceList.size();
				// System.out.println("size:  " + size);
				String[] values = new String[size];
				int i = 0;
				for (String value : occurenceList) {
					values[i] = value;
					// System.out.println("value:  " + value);
					i = i + 1;
				}
				// System.out.println("word:  " + word);
				// System.out.println("nextWord:  " + nextWord);
				job.reduce(word, values, context);
				workerServlet.updateKeysWritten();
				return false;
			}
			workerServlet.updateKeysRead();
			nextWord = line.split("\t")[0];
			if (nextWord.equals(word)) {
				occurenceList.add(line.split("\t")[1]);
			} else {
				String[] values = new String[occurenceList.size()];
				int i = 0;
				for (String value : occurenceList) {
					// System.out.println("value:  " + value);
					values[i] = value;
					i = i + 1;
				}
				// System.out.println("word:  " + word);
				// System.out.println("nextWord:  " + nextWord);
				job.reduce(word, values, context);
				workerServlet.updateKeysWritten();
				occurenceList.clear();
				occurenceList.add(line.split("\t")[1]);
				break;
			}
		}
		return true;
	}

	/* 
	 * Run method
	 * (non-Javadoc)
	 * @see java.lang.Runnable#run()
	 */
	public void run() {
		boolean flag = hasNextLine();
		// System.out.println(flag);
		while (flag) {
			flag = hasNextLine();
		}
	}
}
