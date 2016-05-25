package edu.upenn.cis455.mapreduce.master;

import java.io.PrintWriter;
import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;
import java.util.Map;
import java.util.Map.Entry;

import javax.servlet.http.HttpServlet;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

import edu.upenn.cis455.mapreduce.Client;
import edu.upenn.cis455.mapreduce.WorkerStatus;

public class MasterServlet extends HttpServlet {
	HashMap<String, WorkerStatus> IPAddressPortWorkersMap = new HashMap<String, WorkerStatus>();
	HashMap<String, String> runMapParameters = new HashMap<String, String>();
	// ArrayList<WorkerStatus> activeWorkers = new ArrayList<>();
	public static String WEB_FORM = "<h2>\n<font color=\"blue\">"
			+ "Web Form for submitting jobs.</font>\n<br>\n<br>\n</h2>\n\n"
			+ "<form action=\"/master/runworkers\" method = \"post\">\n  "
			+ "Class Name:<br>\n  <input type=\"text\" name=\"className\" value=\"\">\n "
			+ " <br>\n  Input Directory:<br>\n  <input type=\"text\" name=\"inputDirectory\" value=\"\">\n"
			+ "  <br>\n  Output Directory:<br>\n  <input type=\"text\" name=\"outputDirectory\" value=\"\">\n  <"
			+ "br>\n  Number of Map Threads:<br>\n  <input type=\"text\" name=\"mapThreads\" value=\"\">\n  <br><br>\n "
			+ " Number of Reduce Threads:<br>\n  <input type=\"text\" name=\"reduceThreads\" value=\"\">\n  <br><br>\n  "
			+ "<input type=\"submit\" value=\"Submit\">\n</form></body></html>\n";

	static final long serialVersionUID = 455555001;

	/* 
	 * Do get for master servlet
	 * (non-Javadoc)
	 * @see javax.servlet.http.HttpServlet#doGet(javax.servlet.http.HttpServletRequest, javax.servlet.http.HttpServletResponse)
	 */
	public void doGet(HttpServletRequest request, HttpServletResponse response)
			throws java.io.IOException {
		response.setContentType("text/html");
		PrintWriter out = response.getWriter();
		String path = request.getServletPath();
		// System.out.println("path:" + path);
		switch (path) {

		case "/workerstatus":
			int port = Integer.parseInt(request.getParameter("port"));
			String status = request.getParameter("status");
			String job = request.getParameter("job");
			int keysRead = Integer.parseInt(request.getParameter("keysRead"));
			int keysWritten = Integer.parseInt(request
					.getParameter("keysWritten"));
			String IPAddress = request.getRemoteAddr();
			WorkerStatus workerStatus = new WorkerStatus(IPAddress, port,
					status, job, keysRead, keysWritten);
			IPAddressPortWorkersMap.put(IPAddress + ":" + port, workerStatus);
			if (areAllWorkersWaiting()) {
				Client client = new Client();
				client.clearMaps();
				Long currentTime = new Date().getTime();
				for (Map.Entry<String, WorkerStatus> worker : IPAddressPortWorkersMap
						.entrySet()) {
					if (currentTime - worker.getValue().getLastAccessed() < 30000) {
						client.setUrl("http://" + worker.getKey()
								+ "/worker/runreduce");
						client.setParameters("job", runMapParameters.get("job"));
						client.setParameters("output",
								runMapParameters.get("outputDirectory"));
						client.setParameters("numThreads",
								runMapParameters.get("reduceThreads"));
						client.sendPOSTRequest();
						// System.out.println("worker status sent runreduce!");
					}
				}
			}
			break;

		case "/status":
			out.println("<html>\n<head>\n<style>\ntable, th, td {\n    border: 1px solid green;\n}\nth, td {\n    padding: 15px;\n}\n</style>\n</head>\n<body>\n<table border=\"1\" style=\"width:100%\">");
			out.println("<tr>");
			out.println("<th>IP:Port</th>");
			out.println("<th>Status</th>");
			out.println("<th>Job</th>");
			out.println("<th>Keys Read</th>");
			out.println("<th>Keys Written</th>");
			out.println("</tr>");

			for (Entry<String, WorkerStatus> entry : IPAddressPortWorkersMap
					.entrySet()) {
				Long currentTime = System.currentTimeMillis();
				WorkerStatus worker = entry.getValue();
				if (currentTime - worker.getLastAccessed() < 30000) {
					out.println("<tr>");
					out.println("<td>" + worker.getIPAddress() + ":"
							+ worker.getPort() + "</td>");
					out.println("<td>" + worker.getStatus() + "</td>");
					out.println("<td>" + worker.getJob() + "</td>");
					out.println("<td>" + worker.getKeysRead() + "</td>");
					out.println("<td>" + worker.getKeysWritten() + "</td>");
					out.println("</tr>");
					// activeWorkers.add(worker);
				}

			}
			out.println("</table><br>\n<br>\n<br>");
			out.println(WEB_FORM);

			break;
		default:
			response.sendError(404);
		}
	}

	/* 
	 * Do post for master servlet
	 * (non-Javadoc)
	 * @see javax.servlet.http.HttpServlet#doGet(javax.servlet.http.HttpServletRequest, javax.servlet.http.HttpServletResponse)
	 */
	public void doPost(HttpServletRequest request, HttpServletResponse response)
			throws java.io.IOException {

		response.setContentType("text/html");
		PrintWriter out = response.getWriter();
		String path = request.getServletPath();
		// System.out.println("runmap path:" + path);
		if (path.equals("/runworkers")) {
			runMapParameters.put("job", request.getParameter("className"));
			runMapParameters.put("input",
					request.getParameter("inputDirectory"));
			runMapParameters.put("mapThreads",
					request.getParameter("mapThreads"));
			// runMapParameters.put("numWorkers", activeWorkers.size()+ "");
			runMapParameters.put("outputDirectory",
					request.getParameter("outputDirectory"));
			runMapParameters.put("reduceThreads",
					request.getParameter("reduceThreads"));
			// System.out.println("finished putting params in map");
			long currentTime = System.currentTimeMillis();
			int activeWorkerCount = 0;
			ArrayList<String> activeWorkerList = new ArrayList<String>();

			for (Map.Entry<String, WorkerStatus> worker : IPAddressPortWorkersMap
					.entrySet()) {
				if (currentTime - worker.getValue().getLastAccessed() < 30000) {
					activeWorkerCount++;
					// System.out.println("adding worker" + worker.getKey() +
					// "to list");
					activeWorkerList.add(worker.getKey());
				}
				// System.out.println("tried adding worker.......");
			}

			runMapParameters.put("numWorkers", activeWorkerCount + "");
			// System.out.println("worker count is: " + activeWorkerCount);
			Client client = new Client();
			client.clearMaps();
			// System.out.println("started client");
			currentTime = System.currentTimeMillis();
			for (Map.Entry<String, WorkerStatus> worker : IPAddressPortWorkersMap
					.entrySet()) {
				if (currentTime - worker.getValue().getLastAccessed() < 30000) {
					client.setUrl("http://" + worker.getKey()
							+ "/worker/runmap");
					client.setParameters("job", runMapParameters.get("job"));
					client.setParameters("input", runMapParameters.get("input"));
					client.setParameters("numThreads",
							"" + runMapParameters.get("mapThreads"));
					client.setParameters("numWorkers", "" + activeWorkerCount);
					// System.out.println("setting client params");
					int i = 0;
					for (String w : activeWorkerList) {
						i++;
						client.setParameters("worker" + i, w);
						// System.out.println("worker" + i + w);
					}

					client.sendPOSTRequest();
					// System.out.println("sentRequest for runMap");
				}
			}

		}

	}

	/**
	 * To check if all workers are waiting
	 * @return
	 */
	public boolean areAllWorkersWaiting() {
		long currentTime = new Date().getTime();
		for (Map.Entry<String, WorkerStatus> worker : IPAddressPortWorkersMap
				.entrySet()) {
			if (currentTime - worker.getValue().getLastAccessed() < 30000) {
				if (!worker.getValue().getStatus().equals("waiting")) {
					return false;
				}

			}
		}
		return true;
	}
}
