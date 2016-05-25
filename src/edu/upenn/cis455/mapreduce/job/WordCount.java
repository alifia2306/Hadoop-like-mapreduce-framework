package edu.upenn.cis455.mapreduce.job;

import java.util.StringTokenizer;

import jdk.nashorn.internal.codegen.Emitter;
import edu.upenn.cis455.mapreduce.Context;
import edu.upenn.cis455.mapreduce.Job;

public class WordCount implements Job {

	/* 
	 * Map implementation
	 * (non-Javadoc)
	 * @see edu.upenn.cis455.mapreduce.Job#map(java.lang.String, java.lang.String, edu.upenn.cis455.mapreduce.Context)
	 */
	public void map(String key, String value, Context context) {
		// key: line number
		// value: contents of line
		StringTokenizer tokenizer = new StringTokenizer(value);
		while (tokenizer.hasMoreTokens()) {
			String word = tokenizer.nextToken();
			context.write(word, "1");
		}
	}

	/* 
	 * Reduce implementation
	 * (non-Javadoc)
	 * @see edu.upenn.cis455.mapreduce.Job#map(java.lang.String, java.lang.String, edu.upenn.cis455.mapreduce.Context)
	 */
	public void reduce(String key, String[] values, Context context) {
		// key: word
		// values: list of counts
		if (key.equals("") || values.equals(null)) {
			return;
		}

		System.out.println("key" + key);

		int result = 0;
		for (String v : values) {
			result = result + Integer.parseInt(v);
			System.out.println("value" + v);
		}
		context.write(key, Integer.toString(result));

	}

}
