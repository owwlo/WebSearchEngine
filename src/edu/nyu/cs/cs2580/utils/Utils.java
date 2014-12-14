
package edu.nyu.cs.cs2580.utils;

import java.util.HashMap;
import java.util.Map;

public class Utils {
	
	public static int wordDistance(String word1,String word2){
		 
		  int[][] buffer=new int[word1.length()+1][word2.length()+1];
	        for (int i=0;i<word2.length()+1;i++)
	          buffer[0][i]=i;
	        for (int i=0;i<word1.length()+1;i++)
	          buffer[i][0]=i;
	        for (int i=0;i<word1.length();i++)
	        {
	            for (int j=0;j<word2.length();j++)
	            {
	                if (word1.charAt(i)==word2.charAt(j))
	                  buffer[i+1][j+1]=buffer[i][j];
	                else
	                {
	                    buffer[i+1][j+1]=Math.min(buffer[i][j],Math.min(buffer[i+1][j],buffer[i][j+1]))+1;
	                }
	            }
	        }
	        return buffer[word1.length()][word2.length()];
	}
    public static Map<Integer, String> generateIdTermMap(Map<String, Integer> termIdMap) {
        Map<Integer, String> idTermMap = new HashMap<Integer, String>();
        for (String term : termIdMap.keySet()) {
            int id = termIdMap.get(term);
            if (idTermMap.containsKey(id)) {
                System.err.println("Term id not unique for id: " + id + ", currentTerm: "
                        + idTermMap.get(id) + ", lastTerm: " + term);
            }
            idTermMap.put(id, term);
        }
        return idTermMap;
    }
}
