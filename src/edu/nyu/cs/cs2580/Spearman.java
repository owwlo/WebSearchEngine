package edu.nyu.cs.cs2580;

import java.io.BufferedReader;
import java.io.FileInputStream;
import java.io.InputStreamReader;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class Spearman {
	private static class SpearObject implements Comparable{
		public String docName;
		public double val;
		public SpearObject(String docName,double val){
			this.docName=docName;
			this.val=val;
		}
		public int compareTo(Object target){
			SpearObject to =(SpearObject)target;
			if (Math.abs(val-to.val)<0.0001){
				return (docName.compareTo(to.docName)*(1));
			}
			if (val<to.val)
				return 1;
			else
				return -1;
		}
	}
	
	public static void main(String[] args){
		//args[0] path to pageranks
		//args[1] path to numViews
		if (args.length<2){
			System.out.println("Please provide the path to pageranks and numviews\n");
			return;
		}
		List<SpearObject> pageRank=new ArrayList<SpearObject>();
		List<SpearObject> numViews=new ArrayList<SpearObject>();
		try {
			FileInputStream fis = new FileInputStream(args[0]);
			BufferedReader br = new BufferedReader(new InputStreamReader(fis));
			String line = null;
			while ((line = br.readLine()) != null) {
				String[] splits=line.split(" ");
				SpearObject spObj=new SpearObject(splits[0],Double.valueOf(splits[1]));
				pageRank.add(spObj);
			}
			br.close();
			fis = new FileInputStream(args[1]);
			br = new BufferedReader(new InputStreamReader(fis));
			line = null;
			while ((line = br.readLine()) != null) {
				String[] splits=line.split(" ");
				SpearObject spObj=new SpearObject(splits[0],Double.valueOf(splits[1]));
				numViews.add(spObj);
			}
			br.close();
			
		} catch (Exception e) {
			System.out.println("The path is invalid");
		}
		//Sort into Decreasing Order
		Collections.sort(pageRank);
		Collections.sort(numViews);
		Map<String,Integer> rankMapping=new HashMap<String,Integer> ();
		for (int i=0;i<numViews.size();i++)
			rankMapping.put(numViews.get(i).docName, i);
		//Calculate Z first;
		double Z=0;
		for (int i=0;i<pageRank.size();i++){
			Z+=(double)(i+1);
		}
		Z=Z/pageRank.size();
		double result=0;
		for (int i=0;i<pageRank.size();i++){
			String docName=pageRank.get(i).docName;
			int yPosition=rankMapping.get(docName);
			result+=(double)(i+1-Z)*(double)(yPosition+1-Z);
		}
		double firstDenominator=0;
		for (int i=0;i<pageRank.size();i++){
			firstDenominator+=(i+1-Z)*(i+1-Z);
		}
		//firstDenominator=Math.sqrt(firstDenominator);
		double secondDenominator=0;
		for (int i=0;i<numViews.size();i++){
			secondDenominator+=(i+1-Z)*(i+1-Z);
		}
		//secondDenominator=Math.sqrt(secondDenominator);
		System.out.println(result/Math.sqrt(firstDenominator*secondDenominator));
		
	}

}
