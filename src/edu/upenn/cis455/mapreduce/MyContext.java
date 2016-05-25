package edu.upenn.cis455.mapreduce;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileWriter;
import java.io.IOException;
import java.io.PrintWriter;
import java.io.UnsupportedEncodingException;
import java.math.BigInteger;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.HashMap;

import edu.upenn.cis455.mapreduce.worker.WorkerServlet;

public class MyContext implements Context {
	int numOfWorkers;
	HashMap<Integer, BigInteger> ranges;
	String status = "";
	String spoolOUT = "";
	String output = "";

	/**
	 * Constructor for context implementation
	 * @param worker
	 * @param numWorkers
	 * @param spoolOut
	 */
	public MyContext(WorkerServlet worker, int numWorkers, String spoolOut) {
		this.numOfWorkers = numWorkers;
		ranges = new HashMap<Integer, BigInteger>();
		status = worker.workerParams.get("status");
		// System.out.println("status:  " + status);
		spoolOUT = spoolOut;
	}

	/**
	 * Constructor for context implementation
	 * @param worker
	 * @param numWorkers
	 * @param spoolOut
	 */
	public MyContext(String status, String output) {
		this.status = status;
		this.output = output;
		// System.out.println("outputDIr" + output);
		// System.out.println("status" + status);
	}

	/* Implementing write method
	 * (non-Javadoc)
	 * @see edu.upenn.cis455.mapreduce.Context#write(java.lang.String, java.lang.String)
	 */
	@Override
	public void write(String key, String value) {
		if (key.equals("") || value.equals("")) {
			return;
		}

		// System.out.println("number of workers:  " + numOfWorkers);
		String fileName = "";
		if (status.equals("mapping")) {
			// System.out.println("spoolout directory :   " + spoolOUT);

			// System.out.println("key =  " + key);
			fileName = spoolOUT + "/worker" + selectWorker(key) + ".txt";
			if (!new File(fileName).exists()) {
				File file = new File(spoolOUT + "/worker" + selectWorker(key)
						+ ".txt");
			}

		}

		else {

			fileName = output + "output.txt";
			if (!new File(fileName).exists()) {
				File file = new File(output + "output.txt");
			}
			// System.out.println("complete name:   " + fileName);

		}
		try {
			PrintWriter writer = new PrintWriter(new BufferedWriter(
					new FileWriter(fileName, true)));
			writer.println(key + "\t" + value);
			writer.close();
		} catch (FileNotFoundException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (UnsupportedEncodingException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}

		// TODO Auto-generated method stub

	}

	/**
	 * Generating SHA-1 hash
	 * @param key
	 * @return
	 */
	public String getHash(String key) {
		StringBuffer stringBuilder = null;
		try {
			MessageDigest mDigest = MessageDigest.getInstance("SHA1");

			byte[] d = mDigest.digest(key.getBytes());
			stringBuilder = new StringBuffer();
			for (int i = 0; i < d.length; i++)
				stringBuilder.append(Integer
						.toString((d[i] & 0xff) + 0x100, 16).substring(1));
		} catch (NoSuchAlgorithmException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		// System.out.println("sb" + stringBuilder.toString());
		return stringBuilder.toString();
	}

	/**
	 * Getting Ranges for division
	 */
	public void getRanges() {
		String min_hex = "0000000000000000000000000000000000000000";
		String max_hex = "ffffffffffffffffffffffffffffffffffffffff";
		BigInteger min = new BigInteger(min_hex, 16);
		BigInteger max = new BigInteger(max_hex, 16);
		BigInteger divisions = new BigInteger(Integer.toString(numOfWorkers));
		BigInteger size = max.divide(divisions);
		ranges = new HashMap<Integer, BigInteger>();
		for (int i = 0; i < numOfWorkers; i++) {
			ranges.put(i, min.add(size));
			min = min.add(size);
		}
	}

	/**
	 * Selecting worker based on SHA-1
	 * @param key
	 * @return
	 */
	public int selectWorker(String key) {

		String hash = getHash(key);
		// System.out.println("hash:  " + hash);
		BigInteger hashInt = new BigInteger(hash, 16);
		getRanges();
		for (int i = 0; i < numOfWorkers; i++) {
			if ((hashInt.compareTo(ranges.get(i))) < 0) {
				return i + 1;
			}

		}
		return 0;
	}
}
