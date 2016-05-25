package edu.upenn.cis455.mapreduce;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.OutputStreamWriter;
import java.io.UnsupportedEncodingException;
import java.net.InetAddress;
import java.net.MalformedURLException;
import java.net.Socket;
import java.net.SocketException;
import java.net.SocketTimeoutException;
import java.net.URL;
import java.net.URLEncoder;
import java.util.HashMap;
import java.util.Map;

public class Client {
	HashMap<String, String> paramsMap;
	// private Socket clientSocket;
	public HashMap<String, String> requestMap;
	public HashMap<String, String> responseMap;
	public String responseBody;
	public String body;
	public String urlString;

	// private PrintWriter output;
	// private BufferedReader reader;

	/**
	 * Constructor for client
	 */
	public Client() {
		paramsMap = new HashMap<String, String>();
		requestMap = new HashMap<String, String>();
		responseMap = new HashMap<String, String>();
		// clientSocket = null;
		responseBody = "";
		body = "";
		urlString = "";
		// output = null;
		// reader = null;
	}

	/**
	 * Method to clear Maps
	 */
	public void clearMaps() {
		paramsMap.clear();
		requestMap.clear();
		responseMap.clear();
	}

	/**Method to set URL
	 * @param url
	 */
	public void setUrl(String url) {
		urlString = url;
	}

	/**
	 * Method to set Params
	 * @param paramName
	 * @param paramValue
	 */
	public void setParameters(String paramName, String paramValue) {
		try {
			// System.out.println("paramName: " + paramName);
			// System.out.println("paramValue: " + paramValue);
			paramName = URLEncoder.encode(paramName, "UTF-8");
			paramValue = URLEncoder.encode(paramValue, "UTF-8");
		} catch (UnsupportedEncodingException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}

		paramsMap.put(paramName, paramValue);
	}

	/**
	 * Method to set body
	 * @param body
	 */
	public void setBody(String body) {
		this.body = body;
	}

	/**
	 * Method to send post requestg
	 */
	public void sendPOSTRequest() {
		InetAddress address;
		URL url = null;
		Socket clientSocket = null;
		try {
			url = new URL(urlString);
		} catch (MalformedURLException e1) {
			// TODO Auto-generated catch block
			e1.printStackTrace();
		}
		String host = url.getHost();
		int port = url.getPort();
		// System.out.println("urlstring:  " + urlString);
		try {
			address = InetAddress.getByName(host);
			// clientSocket = new Socket(address.getHostAddress(), port);
			// System.out.println("host address" + address.getHostAddress());
			boolean connected = false;

			while (!connected) {
				try {
					clientSocket = new Socket(address.getHostAddress(), port);
					connected = true;
				} catch (SocketException e) {
					try {
						Thread.sleep(2000);
					} catch (InterruptedException e1) {
						// TODO Auto-generated catch block
						e1.printStackTrace();
					}
				}
			}
			clientSocket.setSoTimeout(10000);
		} catch (Exception e) {
		}

		// System.out.println("is clientsocket null" + (clientSocket == null));
		String line = "";
		try {
			BufferedWriter bufferedWriter = new BufferedWriter(
					new OutputStreamWriter(clientSocket.getOutputStream(),
							"UTF8"));
			bufferedWriter.write("POST " + urlString + " HTTP/1.0\r\n");

			String postBody = "";
			if (body.equals("")) {
				if (!requestMap.containsKey("Content-type")) {
					requestMap.put("Content-Type",
							"application/x-www-form-urlencoded");
				}
				for (Map.Entry<String, String> entry : paramsMap.entrySet()) {
					postBody = postBody + entry.getKey() + "="
							+ entry.getValue() + "&";
				}
				postBody = postBody.substring(0, postBody.length() - 1);
				// System.out.println("postbody:   " + postBody);
			} else {
				if (!requestMap.containsKey("Content-type")) {
					requestMap.put("Content-Type", "text/plain");
				}
				postBody = body;
				// System.out.println("postbody:   " + postBody);
			}
			requestMap.put("Content-Length", "" + postBody.getBytes().length);
			for (Map.Entry<String, String> header : requestMap.entrySet()) {
				bufferedWriter.write(header.getKey() + ": " + header.getValue()
						+ "\r\n");
			}
			bufferedWriter.write("\r\n" + postBody + "\r\n\r\n");
			bufferedWriter.flush();
			BufferedReader bufferedReader = new BufferedReader(
					new InputStreamReader(clientSocket.getInputStream()));
			while ((line = bufferedReader.readLine()) != null) {
				responseBody = responseBody + line;
			}
			// System.out.println("responseBody:   " + responseBody);
			bufferedWriter.close();
			bufferedReader.close();
			clientSocket.close();
		} catch (SocketTimeoutException e) {
			e.printStackTrace();
			return;
		} catch (IOException e) {
			e.printStackTrace();
			return;
		}

	}

	/**
	 * Method to send get request
	 */ 
	public void sendGETRequest() {
		InetAddress address;
		URL url = null;
		Socket clientSocket = null;
		try {
			url = new URL(urlString);
		} catch (MalformedURLException e1) {
			// TODO Auto-generated catch block
			e1.printStackTrace();
		}
		String host = url.getHost();
		int port = url.getPort();
		// System.out.println("urlString:   " + urlString);
		try {
			address = InetAddress.getByName(host);
			boolean connected = false;
			while (!connected) {
				try {
					clientSocket = new Socket(address.getHostAddress(), port);
					connected = true;
				} catch (SocketException e) {
					try {
						Thread.sleep(2000);
					} catch (InterruptedException e1) {
						// TODO Auto-generated catch block
						e1.printStackTrace();
					}
				}
			}
			clientSocket.setSoTimeout(10000);
		} catch (Exception e1) {
			e1.printStackTrace();
		}
		BufferedWriter bufferedWriter;
		BufferedReader bufferedReader;
		responseBody = "";
		String line;
		try {
			bufferedWriter = new BufferedWriter(new OutputStreamWriter(
					clientSocket.getOutputStream(), "UTF8"));

			String requestLine;
			requestLine = "GET " + urlString + "?";
			for (Map.Entry<String, String> param : paramsMap.entrySet()) {
				requestLine = requestLine + param.getKey() + "="
						+ param.getValue() + "&";
			}
			requestLine = requestLine.substring(0, requestLine.length() - 1);
			requestLine = requestLine + " HTTP/1.0\r\n";

			// System.out.println("requestLine:   " + requestLine);
			bufferedWriter.write(requestLine);
			for (Map.Entry<String, String> header : requestMap.entrySet()) {
				bufferedWriter.write(header.getKey() + "=" + header.getValue()
						+ "\r\n");
			}

			bufferedWriter.write("\r\n");
			bufferedWriter.flush();

			bufferedReader = new BufferedReader(new InputStreamReader(
					clientSocket.getInputStream()));
			while ((line = bufferedReader.readLine()) != null) {
				responseBody = responseBody + line;
			}
			// System.out.println("responseBody:   " + responseBody);
			bufferedWriter.close();
			bufferedReader.close();
			clientSocket.close();
		} catch (IOException e) {
			e.printStackTrace();
		}

	}
}
