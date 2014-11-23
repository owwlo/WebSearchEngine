
package edu.nyu.cs.cs2580.utils;

import java.util.HashMap;
import java.util.Map;

public class Utils {
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
