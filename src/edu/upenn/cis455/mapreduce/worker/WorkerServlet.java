package edu.upenn.cis455.mapreduce.worker;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.PrintWriter;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Queue;

import javax.servlet.ServletConfig;
import javax.servlet.ServletException;
import javax.servlet.http.HttpServlet;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

import edu.upenn.cis455.mapreduce.Client;
import edu.upenn.cis455.mapreduce.Context;
import edu.upenn.cis455.mapreduce.Job;
import edu.upenn.cis455.mapreduce.MappingThread;
import edu.upenn.cis455.mapreduce.MyContext;
import edu.upenn.cis455.mapreduce.ReducingThread;
import edu.upenn.cis455.mapreduce.job.WordCount;

public class WorkerServlet extends HttpServlet {

	static final long serialVersionUID = 455555002;
	String IPPortMaster = "";
	String storageDir = "";
	public String spoolInDir = "";
	public String spoolOutDir = "";
	int activeWorkers = 0;
	int port;
	int keysRead = 0;
	int keysWritten = 0;
	public HashMap<String, String> workerParams = new HashMap<>();
	Queue<File> files = new LinkedList<>();
	Queue<String> lines = new LinkedList<>();
	// public MyContext context;
	List<Thread> mapThreadPool = new ArrayList<Thread>();
	List<Thread> reduceThreadPool = new ArrayList<Thread>();
	StatusUpdate statusThreads = null;
	int spoolIncount = 0;

	/* 
	 * Init method for servlet
	 * (non-Javadoc)
	 * @see javax.servlet.GenericServlet#init(javax.servlet.ServletConfig)
	 */
	@Override
	public void init(ServletConfig config) throws ServletException {

		IPPortMaster = config.getInitParameter("master");
		storageDir = config.getInitParameter("storagedir");
		port = Integer.parseInt(config.getInitParameter("port"));
		if (storageDir.endsWith("/")) {
			spoolInDir = storageDir + "spool_in";
			spoolOutDir = storageDir + "spool_out";
		} else {
			spoolInDir = storageDir + "/" + "spool_in";
			spoolOutDir = storageDir + "/" + "spool_out";
		}
		deleteDirectories(new File(spoolInDir));
		deleteDirectories(new File(spoolOutDir));
		File fileSpoolIn = new File(spoolInDir + "/");
		if (!fileSpoolIn.exists()) {
			fileSpoolIn.mkdir();
		}
		File fileSpoolOut = new File(spoolOutDir + "/");
		if (!fileSpoolOut.exists()) {
			fileSpoolOut.mkdir();
		}
		for (int i = 1; i <= activeWorkers; i++) {
			new File(spoolOutDir + "/worker" + i + ".txt");
		}

		// for(File f: fileSpoolIn.listFiles()) f.delete();
		// for(File f: fileSpoolOut.listFiles()) f.delete();

		workerParams.put("port", port + "");
		workerParams.put("status", "idle");
		workerParams.put("job", "");
		workerParams.put("keysRead", keysRead + "");
		workerParams.put("keysWritten", keysWritten + "");

		statusThreads = new StatusUpdate(this);
		statusThreads.start();
		// for(int i = 1; i <= activeWorkers; i++) {
		// new File(spoolOutDir + "/worker" + i + ".txt");
		// }
	}

	/**
	 * Method to delete directory
	 * @param directory
	 */
	private void deleteDirectories(File directory) {
		if (!directory.exists()) {
			return;
		}
		if (!(directory.listFiles().length == 0)) {
			for (File file : directory.listFiles()) {
				if (file.isDirectory())
					deleteDirectories(file);
				else
					file.delete();
			}
		}
		directory.delete();
	}

	/* 
	 * Doget method for WorkerServlet
	 * (non-Javadoc)
	 * @see javax.servlet.http.HttpServlet#doGet(javax.servlet.http.HttpServletRequest, javax.servlet.http.HttpServletResponse)
	 */
	public void doGet(HttpServletRequest request, HttpServletResponse response)
			throws java.io.IOException {
		response.setContentType("text/html");
		PrintWriter out = response.getWriter();
		out.println("<html><head><title>Worker</title></head>");
		out.println("<body>Hi, I am the worker!</body></html>");
	}

	/* 
	 * Dopost method for WorkerServlet
	 * (non-Javadoc)
	 * @see javax.servlet.http.HttpServlet#doGet(javax.servlet.http.HttpServletRequest, javax.servlet.http.HttpServletResponse)
	 */
	public void doPost(HttpServletRequest request, HttpServletResponse response)
			throws java.io.IOException {

		response.setContentType("text/html");
		PrintWriter out = response.getWriter();
		String path = request.getServletPath();
		// System.out.println("path:  " + path);
		switch (path) {

		case "/runmap":
			runMap(request, response);
			break;
		case "/pushdata":
			pushData(request, response);
			break;
		case "/runreduce":
			runReduce(request, response);
			break;
		}

	}

	/**
	 * Method to run map
	 * @param request
	 * @param response
	 */
	public void runMap(HttpServletRequest request, HttpServletResponse response) {
		String job = request.getParameter("job");
		workerParams.put("status", "mapping");
		// System.out.println("status mapping");
		keysRead = 0;
		keysWritten = 0;
		workerParams.put("keysRead", keysRead + "");
		workerParams.put("job", job);
		workerParams.put("keysWritten", keysWritten + "");
		// keysRead = 0;
		// keysWritten = 0;
		String input = request.getParameter("input");
		int numThreads = Integer.parseInt(request.getParameter("numThreads"));
		activeWorkers = Integer.parseInt(request.getParameter("numWorkers"));

		for (int i = 1; i <= activeWorkers; i++) {
			workerParams.put("worker" + i, request.getParameter("worker" + i));
		}
		String inputDirectory = "";
		if (input.startsWith("/")) {
			input = input.substring(1, input.length());
		}

		if (storageDir.endsWith("/")) {
			inputDirectory = storageDir + input + "/";
		} else {
			inputDirectory = storageDir + "/" + input + "/";
		}

		for (File file : new File(inputDirectory).listFiles()) {
			files.add(file);
		}

		Context context = new MyContext(this, activeWorkers, spoolOutDir);
		MappingThread mapThread = new MappingThread(this, context);

		for (int i = 0; i < numThreads; i++) {
			Thread thread = new Thread(mapThread);
			mapThreadPool.add(thread);
			thread.start();
		}

		while (!isMappingDone())
			;

		// for(int i = 1; i <= activeWorkers; i++) {
		// new File(spoolOutDir + "/worker" + i + ".txt");
		// }

		for (Map.Entry<String, String> worker : workerParams.entrySet()) {

			if (worker.getKey().startsWith("worker")) {
				Client client = new Client();
				client.clearMaps();
				String url = "http://" + worker.getValue() + "/worker/pushdata";
				// System.out.println("url " + url);

				client.setUrl(url);
				String data = "";
				String file = spoolOutDir + "/" + worker.getKey() + ".txt";
				BufferedReader bufferedReader;
				try {
					bufferedReader = new BufferedReader(new FileReader(file));

					String line;
					while ((line = bufferedReader.readLine()) != null) {
						if (line.equals("") || line.equals("\n")) {
							continue;
						}
						data = data + line + "\n";

					}
					// System.out.println("data:  " + data);

					bufferedReader.close();
				} catch (FileNotFoundException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				} catch (IOException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				}
				client.setBody(data);
				client.sendPOSTRequest();
			}
		}
		// System.out.println("status waiting");
		workerParams.put("status", "waiting");
		statusThreads.interrupt();
	}

	/**
	 * Synchronized method to check if file is empty
	 * @return
	 */
	public synchronized boolean isFileQueueEmpty() {
		return files.isEmpty();
	}

	/**
	 * Synchronized method to get file
	 * @return
	 */
	public synchronized File getFile() {
		if (!isFileQueueEmpty())
			return files.remove();
		else
			return null;
	}

	/**
	 * Synchronized method to add line
	 * @return
	 */
	public synchronized void addLine(String line) {
		lines.add(line);
	}

	/**
	 * Synchronized method to check if line queue is empty
	 * @return
	 */
	public synchronized boolean isLineQueueEmpty() {
		return lines.isEmpty();
	}

	/**
	 * Synchronized method to get line
	 * @return
	 */
	public synchronized String getLine() {
		if (!isLineQueueEmpty()) {
			return lines.remove();
		}

		else {
			return null;
		}

	}

	/**
	 * Synchronized method to update keys read
	 * @return
	 */
	public synchronized void updateKeysRead() {
		keysRead++;
		workerParams.put("keysRead", keysRead + "");
	}

	/**
	 * Synchronized method to update keys written
	 * @return
	 */
	public synchronized void updateKeysWritten() {
		keysWritten++;
		workerParams.put("keysWritten", keysWritten + "");
	}

	/**Method to pushdata
	 * @param request
	 * @param response
	 */
	public void pushData(HttpServletRequest request,
			HttpServletResponse response) {
		try {
			String data = "";
			BufferedReader reader = request.getReader();
			String line;
			while ((line = reader.readLine()) != null) {
				data = data + line + "\n";
			}
			reader.close();
			spoolIncount++;
			File file = new File(spoolInDir + "/file" + spoolIncount + ".txt");
			String fileName = spoolInDir + "/file" + spoolIncount + ".txt";

			PrintWriter out = new PrintWriter(fileName, "UTF-8");
			out.println(data);
			out.close();
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}

	}

	/**
	 * Method to run reduce threads
	 * @param request
	 * @param response
	 */
	public void runReduce(HttpServletRequest request,
			HttpServletResponse response) {
		String job = request.getParameter("job");
		workerParams.put("status", "reducing");
		workerParams.put("keysRead", "0");
		workerParams.put("job", job);
		workerParams.put("keysWritten", "0");
		// System.out.println("status reducing");
		keysRead = 0;
		keysWritten = 0;
		String output = request.getParameter("output");
		int numThreads = Integer.parseInt(request.getParameter("numThreads"));
		// int numWorkers =
		// Integer.parseInt(request.getParameter("numWorkers"));

		// for (int i = 1; i <= numWorkers; i++) {
		// workerParams.put("worker"+i, request.getParameter("worker"+i));
		// }
		String outputDirectory = "";
		if (output.startsWith("/")) {
			output = output.substring(1, output.length());
		}

		if (storageDir.endsWith("/")) {
			outputDirectory = storageDir + output + "/";
		} else {
			outputDirectory = storageDir + "/" + output + "/";
		}

		File file = new File(outputDirectory);
		if (!file.exists()) {
			file.mkdir();
		} else {
			deleteDirectories(file);
			file.mkdir();
		}

		// combinign all files
		combineMapResults(spoolInDir);
		String combinedFile = spoolInDir + "/combined.txt";

		// Sorting files.
		BufferedReader bufferedReader = null;

		Process p = null;
		try {
			p = Runtime.getRuntime().exec("sort " + combinedFile);
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		bufferedReader = new BufferedReader(new InputStreamReader(
				p.getInputStream()));
		File f = new File(spoolInDir + "/sorted.txt");
		FileWriter fileWriter;
		try {
			fileWriter = new FileWriter(f.getAbsoluteFile());
			BufferedWriter bufferedWriter = new BufferedWriter(fileWriter);
			String line;
			while ((line = bufferedReader.readLine()) != null) {

				// System.out.println("line:  " + line);
				if (line.trim().equals("")) {
					continue;
				}

				bufferedWriter.write(line + "\n");
			}
			bufferedWriter.close();
			bufferedReader.close();
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		// System.out.println("outputdir   " + outputDirectory);
		Context context = new MyContext(workerParams.get("status"),
				outputDirectory);
		// File outputTextFile = new File(outputDirectory + "/output.txt");
		ReducingThread reduceThread = new ReducingThread(this, context,
				spoolInDir);

		for (int i = 0; i < numThreads; i++) {
			Thread thread = new Thread(reduceThread);
			reduceThreadPool.add(thread);
			thread.start();
		}

		while (!isReducingDone())
			;
		workerParams.put("status", "idle");
		keysRead = 0;
		workerParams.put("keysRead", keysRead + "");
		// System.out.println("status idle again");
		statusThreads.interrupt();
	}

	/**
	 * Method to check if mapping is done
	 * @return
	 */
	public boolean isMappingDone() {
		for (Thread thread : mapThreadPool) {
			if (thread.isAlive()) {
				return false;
			}
		}
		return true;
	}

	/**
	 * Method to check if reduce is done
	 * @return
	 */
	public boolean isReducingDone() {
		for (Thread thread : reduceThreadPool) {
			if (thread.isAlive()) {
				return false;
			}
		}
		return true;
	}

	/**
	 * Method to combine map results
	 * @param spoolInDir
	 */
	private void combineMapResults(String spoolInDir) {
		File directory = new File(spoolInDir);
		File file = new File(spoolInDir + "/combined.txt");
		// System.out.println(spoolInDir+"/combined.txt");

		PrintWriter out;
		try {
			out = new PrintWriter(spoolInDir + "/combined.txt", "UTF-8");
			String fileData = "";
			for (File f : directory.listFiles()) {
				if (f.getName().startsWith("file")) {
					// System.out.println(f.getAbsolutePath());
					fileData = fileData + getData(f.getAbsolutePath());
				}
			}
			out.println(fileData);
			out.close();
		} catch (IOException e) {
			// e.printStackTrace();
		}
	}

	/**
	 * Method to get data from a file
	 * @param file
	 * @return
	 */
	private String getData(String file) {
		BufferedReader bufferedReader;
		String data = "";
		try {
			bufferedReader = new BufferedReader(new FileReader(file));

			String line;
			while ((line = bufferedReader.readLine()) != null) {
				// System.out.println("line:  " + line);
				if (line.equals("") || line.equals("\n")) {
					continue;
				}
				data = data + line + "\n";

			}

			bufferedReader.close();
		} catch (FileNotFoundException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		return data;
	}

}
