
package edu.nyu.cs.cs2580.utils;

import java.util.Comparator;
import edu.nyu.cs.cs2580.ScoredDocument;

public class ScoredDocumentComparator implements Comparator<ScoredDocument> {
    @Override
    public int compare(ScoredDocument o1, ScoredDocument o2) {
        return (o1._score > o2._score) ? -1 : 1;
    }
}
