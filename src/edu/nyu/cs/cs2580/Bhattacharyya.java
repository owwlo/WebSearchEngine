package edu.nyu.cs.cs2580;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Set;

public class Bhattacharyya {

	public static class Pair<A, B> {
		public A first;
		public B second;

		public Pair(A first, B second) {
			super();
			this.first = first;
			this.second = second;
		}

		public Pair(Map.Entry<A, B> e) {
			super();
			this.first = e.getKey();
			this.second = e.getValue();
		}

		public int hashCode() {
			int hashFirst = first != null ? first.hashCode() : 0;
			int hashSecond = second != null ? second.hashCode() : 0;
			return (hashFirst + hashSecond) * hashSecond + hashFirst;
		}

		public boolean equals(Object other) {
			if (other instanceof Pair) {
				Pair otherPair = (Pair) other;
				return (this.first == otherPair.first || (this.first != null
						&& otherPair.first != null && this.first
							.equals(otherPair.first)))
						&& (this.second == otherPair.second || (this.second != null
								&& otherPair.second != null && this.second
									.equals(otherPair.second)));
			}
			return false;
		}

		public String toString() {
			return "(" + first + "," + second + ")";
		}
	}

	private Map<String, Map<String, Double>> expandedQuery = new HashMap<String, Map<String, Double>>();
	private Map<Pair<String, String>, Double> coeff = new LinkedHashMap<Pair<String, String>, Double>();

	public Map<String, Double> loadFile(String filename) {
		Map<String, Double> expanded = new HashMap<String, Double>();

		try {
			BufferedReader reader = new BufferedReader(new FileReader(filename));
			String line;
			while ((line = reader.readLine()) != null) {
				String[] entry = line.split("\t");
				String term = entry[0];
				double prob = Double.parseDouble(entry[1]);
				expanded.put(term, prob);
			}
			reader.close();
		} catch (FileNotFoundException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (IOException e) {
			e.printStackTrace();
		}

		return expanded;
	}

	public void loadPath(String filename) {
		try {
			BufferedReader reader = new BufferedReader(new FileReader(filename));
			String line;
			while ((line = reader.readLine()) != null) {
				String[] pair = line.split(":");
				String query = pair[0];
				String file = pair[1];
				Map<String, Double> expanded = loadFile(file);
				expandedQuery.put(query, expanded);
			}
			reader.close();
		} catch (FileNotFoundException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (IOException e) {
			e.printStackTrace();
		}
	}

	public void write2file(String filename) {
		try {
			BufferedWriter writer = new BufferedWriter(new FileWriter(filename));
			for (Map.Entry<Pair<String, String>, Double> e : coeff.entrySet()) {
				Pair<String, String> p = e.getKey();
				writer.write(p.first + "\t" + p.second + "\t" + e.getValue()
						+ "\n");
			}
			writer.close();
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}

	}

	public void computeSim() {
		Set<String> set = expandedQuery.keySet();

		for (String q1 : set) {
			Map<String, Double> terms1 = expandedQuery.get(q1);
			for (String q2 : set) {
				if (q1.equals(q2))
					continue;

				Pair<String, String> key = new Pair<String, String>(q1, q2);
				Pair<String, String> partner = new Pair<String, String>(q2, q1);

				if (coeff.containsKey(partner)) {
					coeff.put(key, coeff.get(partner));
					continue;
				}
				double beta = 0.0;
				Map<String, Double> terms2 = expandedQuery.get(q2);
				// iterate over all terms
				for (Map.Entry<String, Double> e : terms1.entrySet()) {
					String term = e.getKey();
					if (terms2.containsKey(term)) {
						beta += Math.sqrt(terms1.get(term) * terms2.get(term));
					}
				}

				coeff.put(key, beta);
			}
		}

	}

	public static void main(String[] args) throws IOException {
		if (args.length != 2) {
			System.err
					.println("Usage: java -cp src edu.nyu.cs.cs2580.Bhattacharyya prf.tsv qsim.tsv");
		}

		String inputPath = args[0];
		String outputPath = args[1];

		Bhattacharyya bha = new Bhattacharyya();
		bha.loadPath(inputPath);
		bha.computeSim();
		bha.write2file(outputPath);
	}
}
