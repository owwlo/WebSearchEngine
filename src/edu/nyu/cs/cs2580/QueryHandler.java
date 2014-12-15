
package edu.nyu.cs.cs2580;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.math.BigInteger;

import com.google.gson.Gson;
import com.sun.net.httpserver.Headers;
import com.sun.net.httpserver.HttpExchange;
import com.sun.net.httpserver.HttpHandler;

import edu.nyu.cs.cs2580.Bhattacharyya.Pair;
import edu.nyu.cs.cs2580.SearchEngine.Options;
import edu.nyu.cs.cs2580.utils.AssistantIndexBuilder;
import edu.nyu.cs.cs2580.utils.ClickLoggingManager;
import edu.nyu.cs.cs2580.utils.ScoredDocumentComparator;

import java.security.SecureRandom;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.PriorityQueue;
import java.util.Set;
import java.util.Vector;

/**
 * Handles each incoming query, students do not need to change this class except
 * to provide more query time CGI arguments and the HTML output. N.B. This class
 * is not thread-safe.
 * 
 * @author congyu
 * @author fdiaz
 */
public class QueryHandler implements HttpHandler {

    /**
     * CGI arguments provided by the user through the URL. This will determine
     * which Ranker to use and what output format to adopt. For simplicity, all
     * arguments are publicly accessible.
     */
    public static Set<String> stopWords = new HashSet<String>(
            Arrays.asList(new String[] {
                    // from nltk
                    "i", "me", "my", "myself", "we", "our", "ours",
                    "ourselves", "you", "your", "yours", "yourself",
                    "yourselves", "he", "him", "his", "himself", "she", "her",
                    "hers", "herself", "it", "its", "itself", "they", "them",
                    "their", "theirs", "themselves", "what", "which", "who",
                    "whom", "this", "that", "these", "those", "am", "is",
                    "are", "was", "were", "be", "been", "being", "have", "has",
                    "had", "having", "do", "does", "did", "doing", "a", "an",
                    "the", "and", "but", "if", "or", "because", "as", "until",
                    "while", "of", "at", "by", "for", "with", "about",
                    "against", "between", "into", "through", "during",
                    "before", "after", "above", "below", "to", "from", "up",
                    "down", "in", "out", "on", "off", "over", "under", "again",
                    "further", "then", "once", "here", "there", "when",
                    "where", "why", "how", "all", "any", "both", "each", "few",
                    "more", "most", "other", "some", "such", "no", "nor",
                    "not", "only", "own", "same", "so", "than", "too", "very",
                    "s", "t", "can", "will", "just", "don", "should",
                    "now",
                    // from statistics
                    "many", "\"the", "?", "1", "b", "&", "", "wikipedia",
                    "also", "[edit]", "^", "[1]", "n/a", "wa", "us", "thi",
                    "ha", "ye", "hi"
            }));

    public static class CgiArguments {
        // The raw user query
        public String _query = "";
        // How many results to return
        private int _numResults = 10;

        // hw3 prf: top m terms from top k documents
        private int _numDocs = 10;
        private int _numTerms = 5;

        private boolean _isRedirect = false;

        // The type of the ranker we will be using.
        public enum RankerType {
            NONE, FULLSCAN, CONJUNCTIVE, FAVORITE, COSINE, PHRASE, QL, LINEAR, COMPREHENSIVE
        }

        public RankerType _rankerType = RankerType.NONE;

        // The output format.
        public enum OutputFormat {
            TEXT, HTML,
        }

        public OutputFormat _outputFormat = OutputFormat.TEXT;

        public CgiArguments(String uriQuery) {
            String[] params = uriQuery.split("&");
            for (String param : params) {
                String[] keyval = param.split("=", 2);
                if (keyval.length < 2) {
                    continue;
                }
                String key = keyval[0].toLowerCase();
                String val = keyval[1];
                if (key.equals("query")) {
                    _query = val;
                } else if (key.equals("redirect")) {
                    _isRedirect = true;
                } else if (key.equals("num")) {
                    try {
                        _numResults = Integer.parseInt(val);
                    } catch (NumberFormatException e) {
                        // Ignored, search engine should never fail upon invalid
                        // user input.
                    }
                } else if (key.equals("ranker")) {
                    try {
                        _rankerType = RankerType.valueOf(val.toUpperCase());
                    } catch (IllegalArgumentException e) {
                        // Ignored, search engine should never fail upon invalid
                        // user input.
                    }
                } else if (key.equals("format")) {
                    try {
                        _outputFormat = OutputFormat.valueOf(val.toUpperCase());
                    } catch (IllegalArgumentException e) {
                        // Ignored, search engine should never fail upon invalid
                        // user input.
                    }
                } else if (key.equals("numdocs")) {
                    // hw3 prf
                    try {
                        _numDocs = Integer.parseInt(val);
                    } catch (NumberFormatException e) {
                        // Ignored
                    }
                } else if (key.equals("numterms")) {
                    // hw3 prf
                    try {
                        _numTerms = Integer.parseInt(val);
                    } catch (NumberFormatException e) {
                        // Ignored
                    }
                }
            } // End of iterating over params
        }
    }

    // hw3 prf: for priority queue of mapentry<term, occ>
    private class CompareByValue implements
            Comparator<Map.Entry<String, Double>> {
        @Override
        public int compare(Map.Entry<String, Double> lhs,
                Map.Entry<String, Double> rhs) {
            return lhs.getValue().compareTo(rhs.getValue());
        }
    }

    // For accessing the underlying documents to be used by the Ranker. Since
    // we are not worried about thread-safety here, the Indexer class must take
    // care of thread-safety.
    private Indexer _indexer;

    private Options _options;

    public QueryHandler(Options options, Indexer indexer) {
        _indexer = indexer;
        _options = options;
    }

    private void respondWithMsg(HttpExchange exchange, final String message)
            throws IOException {
        Headers responseHeaders = exchange.getResponseHeaders();
        responseHeaders.set("Content-Type", "text/plain");
        exchange.sendResponseHeaders(200, 0); // arbitrary number of bytes
        OutputStream responseBody = exchange.getResponseBody();
        responseBody.write(message.getBytes());
        responseBody.close();
    }

    private void constructTextOutput(final Vector<ScoredDocument> docs,
            StringBuffer response) {
        for (ScoredDocument doc : docs) {
            response.append(response.length() > 0 ? "\n" : "");
            response.append(doc.asTextResult());
        }
        response.append(response.length() > 0 ? "\n" : "");
    }

    private void constructTextOutput(final Map<String, List<Query>> queries,
            StringBuffer response) {
        Map<String, List<String>> mp = new HashMap<String, List<String>>();
        for (String name : queries.keySet()) {
            List<Query> qs = queries.get(name);
            List<String> sl = new ArrayList<String>();
            for (Query q : qs) {
                sl.add(q.toString());
            }
            mp.put(name, sl);
        }
        String json = new Gson().toJson(mp);
        response.append(json);
    }

    // hw3 prf: query representation output
    private void constructTermOutput(
            final ArrayList<Pair<String, Double>> terms, StringBuffer response) {
        for (int i = terms.size() - 1; i >= 0; --i) {
            Pair<String, Double> p = terms.get(i);
            response.append(p.first + "\t" + p.second + "\n");
        }
    }

    public static Map<String, String> getQueryMap(String query) {
        String[] params = query.split("&");
        Map<String, String> map = new HashMap<String, String>();
        for (String param : params) {
            String name = param.split("=")[0];
            String value = param.split("=")[1];
            map.put(name, value);
        }
        return map;
    }

    synchronized private String getSession(HttpExchange exchange) {
        String session = (String) exchange.getAttribute("session");
        if (session == null) {
            SecureRandom random = new SecureRandom();
            session = new BigInteger(130, random).toString(32);
            exchange.setAttribute("session", session);
        }
        return (String) exchange.getAttribute("session");
    }

    public void handle(HttpExchange exchange) throws IOException {
        String requestMethod = exchange.getRequestMethod();
        if (!requestMethod.equalsIgnoreCase("GET")) { // GET requests only.
            return;
        }

        // Print the user request header.
        Headers requestHeaders = exchange.getRequestHeaders();
        System.out.print("Incoming request: ");
        for (String key : requestHeaders.keySet()) {
            System.out.print(key + ":" + requestHeaders.get(key) + "; ");
        }
        String uriQuery = exchange.getRequestURI().getQuery();
        String uriPath = exchange.getRequestURI().getPath();

        if (uriPath == null || uriQuery == null) {
            respondWithMsg(exchange, "Something wrong with the URI!");
        }

        System.out.println("Query: " + uriQuery);

        if (uriPath.equals("/click_loging")) {
            Map<String, String> query_map = getQueryMap(uriQuery);
            Set<String> keys = query_map.keySet();
            if (keys.contains("did") && keys.contains("query")
                    && keys.contains("ranker") && keys.contains("action")) {
                ClickLoggingManager clm = ClickLoggingManager.getInstance();

                // Ensure the session will be the same when open multiple
                // threads
                synchronized (clm) {
                    String session = getSession(exchange);
                    clm.writeToLog(session, query_map.get("query"),
                            query_map.get("did"), query_map.get("action"));

                }
                respondWithMsg(exchange, "Success!");
            }
        } else if (uriPath.equals("/search")) {
            // Add session id for the sessionhandler
            SessionHandler handler = SessionHandler.getInstance();
            synchronized (handler) {
                String session = getSession(exchange);
                handler.setSession(session);
            }

            // Process the CGI arguments.
            CgiArguments cgiArgs = new CgiArguments(uriQuery);
            if (cgiArgs._query.isEmpty()) {
                respondWithMsg(exchange, "No query is given!");
            }

            // Create the ranker.
            Ranker ranker = Ranker.Factory.getRankerByArguments(cgiArgs,
                    SearchEngine.OPTIONS, _indexer);
            if (ranker == null) {
                respondWithMsg(exchange,
                        "Ranker " + cgiArgs._rankerType.toString()
                                + " is not valid!");
            }

            // Processing the query.
            Query processedQuery = new QueryPhrase(cgiArgs._query);
            processedQuery.processQuery();

            if (cgiArgs._isRedirect) {
                handler.querySearch(cgiArgs._query);
            }

            // Ranking.
            Vector<ScoredDocument> scoredDocs = ranker.runQuery(processedQuery,
                    cgiArgs._numResults);
            StringBuffer response = new StringBuffer();
            switch (cgiArgs._outputFormat) {
                case TEXT:
                    constructTextOutput(scoredDocs, response);
                    break;
                case HTML:

                    // Sort result of ScoredDocuments
                    Collections.sort(scoredDocs, new ScoredDocumentComparator());
                    Map<String, String> query_map = getQueryMap(uriQuery);
                    // Read initial html file
                    File htmlFile = new File("./public/index.html");
                    if (!htmlFile.exists()) {
                        htmlFile = new File("../public/index.html");
                    }

                    InputStream fileIn = new FileInputStream(htmlFile);
                    byte[] data = new byte[(int) htmlFile.length()];
                    fileIn.read(data);
                    fileIn.close();
                    String htmlStr = new String(data, "UTF-8");

                    // Write parameters into it
                    htmlStr = htmlStr.replace("{{query}}", query_map.get("query"));
                    htmlStr = htmlStr
                            .replace("{{ranker}}", query_map.get("ranker"));
                    htmlStr = htmlStr.replace("{{count}}",
                            String.valueOf(scoredDocs.size()));

                    StringBuilder sb = new StringBuilder();
                    for (int i = 0; i < 10 && i < scoredDocs.size(); i++) {
                        ScoredDocument sd = scoredDocs.get(i);
                        sb.append(sd.asHtmlResult());
                    }
                    htmlStr = htmlStr.replace("{{result}}", sb.toString());

                    Headers responseHeaders = exchange.getResponseHeaders();
                    // Make browser this it is a HTML
                    responseHeaders.set("Content-Type", "text/html");
                    exchange.sendResponseHeaders(200, 0);
                    OutputStream responseBody = exchange.getResponseBody();
                    responseBody.write(htmlStr.getBytes());
                    responseBody.close();
                    return;
                default:
                    // nothing
            }
            respondWithMsg(exchange, response.toString());
            System.out.println("Finished query: " + cgiArgs._query);
        } else if (uriPath.equals("/page_summary")) {
            CgiArguments cgiArgs = new CgiArguments(uriQuery);
            int docid = cgiArgs._numResults;
            AssistantIndexBuilder aib = AssistantIndexBuilder.getInstance(_options);
            List<String> cachedTerms = aib.getCacheKeeper().getCachedPage(docid);
            StringBuilder sb = new StringBuilder();
            for (int i = 0; i < cachedTerms.size() && i < 50; ++i) {
                sb.append(cachedTerms.get(i));
                sb.append(" ");
            }
            respondWithMsg(exchange, sb.toString());
        } else if (uriPath.equals("/prediction")) {
            // Add session id for the sessionhandler
            SessionHandler handler = SessionHandler.getInstance();
            synchronized (handler) {
                String session = getSession(exchange);
                handler.setSession(session);
            }

            CgiArguments cgiArgs = new CgiArguments(uriQuery);
            if (cgiArgs._query.isEmpty()) {
                respondWithMsg(exchange, "No query is given!");
            }
            // Processing the query.
            Query processedQuery = new QueryPhrase(cgiArgs._query);
            processedQuery.processQuery();

            IndexerInvertedOccurrence iiq = (IndexerInvertedOccurrence) _indexer;
            Map<String, List<Query>> qlst = iiq.querySearch(processedQuery);
            StringBuffer response = new StringBuffer();
            constructTextOutput(qlst, response);
            respondWithMsg(exchange, response.toString());
        } else if (uriPath.equals("/prf")) {
            // query representation

            // Process the CGI arguments.
            CgiArguments cgiArgs = new CgiArguments(uriQuery);
            if (cgiArgs._query.isEmpty()) {
                respondWithMsg(exchange, "No query is given!");
            }

            // Create the ranker.
            Ranker ranker = Ranker.Factory.getRankerByArguments(cgiArgs,
                    SearchEngine.OPTIONS, _indexer);
            if (ranker == null) {
                respondWithMsg(exchange,
                        "Ranker " + cgiArgs._rankerType.toString()
                                + " is not valid!");
            }

            // Processing the query.
            Query processedQuery = new QueryPhrase(cgiArgs._query);
            processedQuery.processQuery();

            // Ranking.
            Vector<ScoredDocument> scoredDocs = ranker.runQuery(processedQuery,
                    cgiArgs._numDocs);
            // System.out.println("get scoredDocs size "+scoredDocs.size());
            StringBuffer response = new StringBuffer();

            // get all the doc, generate a map: term->occ(prob) in all docs
            Map<String, Double> term_map = new HashMap<String, Double>();
            int all_occ = 0; // all term occ in all k docs

            // aggregate over k documents
            for (ScoredDocument sdoc : scoredDocs) {
                // map of terms in a certain doc
                Map<String, Integer> termInDoc = _indexer
                        .documentTermFrequencyMap(sdoc._doc._docid);
                // System.out.println("get empty term set???:"+termInDoc.size());

                for (Map.Entry<String, Integer> entry : termInDoc.entrySet()) {
                    String term = entry.getKey();
                    Integer occ = entry.getValue();

                    /*
                     * if (stopWords.contains(term)) { //
                     * System.out.println("get a stopword "+term); continue; }
                     */

                    // System.out.println("term:occ:"+term+":"+occ);
                    all_occ += occ;
                    if (term_map.containsKey(term)) {
                        term_map.put(term, term_map.get(term) + occ);
                    } else {
                        term_map.put(term, (double) occ);
                    }
                }
            }
            if (all_occ == 0) {
                System.err.println("all_occ == 0 ???");
            }

            // get the top m terms in ascending order
            PriorityQueue<Map.Entry<String, Double>> topTerms = new PriorityQueue<Map.Entry<String, Double>>(
                    cgiArgs._numTerms, new CompareByValue());
            for (Map.Entry<String, Double> entry : term_map.entrySet()) {
                if (stopWords.contains(entry.getKey()))
                    continue;

                topTerms.add(entry);
                if (topTerms.size() > cgiArgs._numTerms) {
                    topTerms.poll();
                }
            }

            // put the top m terms in a stable data structure
            ArrayList<Pair<String, Double>> top_term = new ArrayList<Pair<String, Double>>();
            while (topTerms.size() > 0) {
                top_term.add(new Pair<String, Double>(topTerms.poll()));
            }

            // divide by denominator
            double sum = 0.0;
            for (Pair<String, Double> e : top_term) {
                double prob = e.second / all_occ;
                e.second = prob;
                sum += prob;
            }
            if (sum == 0.0) {
                System.err.println("sum == 0 ???");
            } else {
                // System.out.println("Sum is " + sum);
            }

            // normalize
            for (Pair<String, Double> e : top_term) {
                e.second /= sum;
            }

            constructTermOutput(top_term, response);

            respondWithMsg(exchange, response.toString());
        } else {
            respondWithMsg(exchange, "No valid query is given.");
        }
    }
}
