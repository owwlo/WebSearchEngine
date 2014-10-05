
package edu.nyu.cs.cs2580;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.math.BigInteger;

import com.sun.net.httpserver.Headers;
import com.sun.net.httpserver.HttpExchange;
import com.sun.net.httpserver.HttpHandler;

import edu.nyu.cs.cs2580.utils.ClickLoggingManager;
import edu.nyu.cs.cs2580.utils.ScoredDocumentComparator;

import java.security.SecureRandom;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;
import java.util.Iterator;
import java.util.Vector;

public class QueryHandler implements HttpHandler {
    private Ranker _ranker;

    public QueryHandler(Ranker ranker) {
        _ranker = ranker;
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
        System.out.println();
        StringBuilder queryResponse = new StringBuilder();
        String uriQuery = exchange.getRequestURI().getQuery();
        String uriPath = exchange.getRequestURI().getPath();

        // Construct a simple response.
        Headers responseHeaders = exchange.getResponseHeaders();
        responseHeaders.set("Content-Type", "text/plain");

        if ((uriPath != null) && (uriQuery != null)) {
            if (uriPath.equals("/click_loging")) {
                Map<String, String> query_map = getQueryMap(uriQuery);
                Set<String> keys = query_map.keySet();
                if (keys.contains("did") && keys.contains("query") && keys.contains("ranker")
                        && keys.contains("action")) {
                    ClickLoggingManager clm = ClickLoggingManager.getInstance();

                    // Ensure the session will be the same when open multiple threads
                    synchronized (clm) {
                        String session = (String) exchange.getAttribute("session");
                        if (session == null) {
                            SecureRandom random = new SecureRandom();
                            session = new BigInteger(130, random).toString(32);
                            exchange.setAttribute("session", session);
                        }
                        clm.writeToLog(session, query_map.get("query"), query_map.get("did"),
                                query_map.get("action"));

                    }

                    responseHeaders.set("Content-Type", "text/plain");
                    queryResponse.append("Success!");
                }
            } else if (uriPath.equals("/search")) {
                Map<String, String> query_map = getQueryMap(uriQuery);
                Set<String> keys = query_map.keySet();
                Vector<ScoredDocument> sds = new Vector<ScoredDocument>();
                if (keys.contains("query")) {
                    if (keys.contains("ranker")) {
                        String ranker_type = query_map.get("ranker");
                        // @CS2580: Invoke different ranking functions inside
                        // your
                        // implementation of the Ranker class.
                        /*
                         * if (ranker_type.equals("cosine")){ ///queryResponse =
                         * (ranker_type + " not implemented."); } else if
                         * (ranker_type.equals("QL")){ queryResponse =
                         * (ranker_type + " not implemented."); } else if
                         * (ranker_type.equals("phrase")){ queryResponse =
                         * (ranker_type + " not implemented."); } else if
                         * (ranker_type.equals("linear")){ queryResponse =
                         * (ranker_type + " not implemented."); } else {
                         * queryResponse = (ranker_type+" not implemented."); }
                         */
                        sds = _ranker.runBySignal(ranker_type, query_map.get("query"));
                    }
                    else {
                        sds = _ranker.runquery(query_map.get("query"));
                    }

                    // Sort result of ScoredDocuments
                    Collections.sort(sds, new ScoredDocumentComparator());
                    
                    // We support both HTML and TEXT.
                    // Default format is TEXT.
                    String outputFormat = "text";
                    if (query_map.containsKey("format")) {
                        outputFormat = query_map.get("format");
                    }

                    // Build output
                    if (outputFormat.equalsIgnoreCase("html")) {

                        // Read initial html file
                        File htmlFile = new File("./public/index.html");
                        InputStream fileIn = new FileInputStream(htmlFile);
                        byte[] data = new byte[(int) htmlFile.length()];
                        fileIn.read(data);
                        fileIn.close();
                        String htmlStr = new String(data, "UTF-8");

                        // Write parameters into it
                        htmlStr = htmlStr.replace("{{query}}", query_map.get("query"));
                        htmlStr = htmlStr.replace("{{ranker}}", query_map.get("ranker"));
                        htmlStr = htmlStr.replace("{{count}}", String.valueOf(sds.size()));

                        StringBuilder sb = new StringBuilder();
                        for (int i = 0; i < 10 && i < sds.size(); i++) {
                            ScoredDocument sd = sds.get(i);
                            sb.append(sd._did + "&&&" + sd._score + "&&&" + sd._title + "&&&");
                        }
                        htmlStr = htmlStr.replace("{{result}}", sb.toString());

                        queryResponse.append(htmlStr);

                        // Make browser this it is a HTML
                        responseHeaders.set("Content-Type", "text/html");
                    } else {
                        Iterator<ScoredDocument> itr = sds.iterator();
                        while (itr.hasNext()) {
                            ScoredDocument sd = itr.next();
                            if (queryResponse.length() > 0) {
                                queryResponse.append("\n");
                            }
                            queryResponse.append(query_map.get("query") + "\t"
                                    + sd.asString());
                        }

                        responseHeaders.set("Content-Type", "text/plain");
                    }

                    if (queryResponse.length() > 0) {
                        queryResponse.append("\n");
                    }
                }
            }
        }
        if (queryResponse.length() == 0) {
            queryResponse.append("Nothing returned.");
        }

        exchange.sendResponseHeaders(200, 0); // arbitrary number of bytes
        OutputStream responseBody = exchange.getResponseBody();
        responseBody.write(queryResponse.toString().getBytes());
        responseBody.close();
    }
}
