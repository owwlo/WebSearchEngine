
package edu.nyu.cs.cs2580;

import java.io.IOException;
import java.util.Arrays;
import java.util.List;

/**
 * @CS2580: implement this class for HW2 to handle phrase. If the raw query is
 *          ["new york city"], the presence of the phrase "new york city" must
 *          be recorded here and be used in indexing and ranking.
 */
public class QueryPhrase extends Query {

    public QueryPhrase(String query) {
        super(query);
    }

    @Override
    public void processQuery() {
        this._tokens.clear();
        int startIdx = 0;
        boolean phase = false;
        do {
            int endIdx = _query.indexOf("\"", startIdx);
            if (endIdx == -1) {
                endIdx = _query.length();
                String sentence = _query.substring(startIdx, endIdx);
                _tokens.addAll(Arrays.asList((sentence.replaceAll("\"", "").trim()
                        .split(" "))));
            } else {
                if (phase) {
                    String sentence = _query.substring(startIdx, endIdx);
                    sentence = sentence.replaceAll("\"", "").trim();
                    _tokens.add(sentence);
                    phase = false;
                } else {
                    String sentence = _query.substring(startIdx, endIdx).replaceAll("\"", "")
                            .trim();
                    if (sentence.length() > 0) {
                        _tokens.addAll(Arrays.asList((sentence.split(" "))));
                    }
                    phase = true;
                }
            }
            startIdx = endIdx + 1;
        } while (startIdx < _query.length());
    }

    public static void main(String[] args) throws IOException {
        List<String> testList = Arrays.asList(new String[] {
                "new york city", "\"new york city\" york",
                "new \"york\" city",
                "wo de jia zai \"dong bei song hua jiang shang\"",
                "\"\"",
                "wodejia"
        });
        for (String query : testList) {
            System.out.println("##Test for: " + query);
            Query q = new QueryPhrase(query);
            q.processQuery();
            for (String token : q._tokens) {
            	 Stemmer s = new Stemmer();
                 s.add(token.toLowerCase().toCharArray(), token.length());
                 s.stem();
                System.out.println(s.toString());
            }
        }
    }
}
