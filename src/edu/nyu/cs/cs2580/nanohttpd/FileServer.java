
package edu.nyu.cs.cs2580.nanohttpd;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.net.URI;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import com.sun.net.httpserver.HttpExchange;
import com.sun.net.httpserver.HttpHandler;

import edu.nyu.cs.cs2580.QueryHandler;
import edu.nyu.cs.cs2580.utils.FakeHttpExchange;

public class FileServer extends NanoHTTPD {
    public static final String MIME_DEFAULT_BINARY = "application/octet-stream";
    private static final Map<String, String> MIME_TYPES = new HashMap<String, String>() {
        {
            put("css", "text/css");
            put("htm", "text/html");
            put("html", "text/html");
            put("xml", "text/xml");
            put("txt", "text/plain");
            put("gif", "image/gif");
            put("jpg", "image/jpeg");
            put("jpeg", "image/jpeg");
            put("png", "image/png");
            put("js", "application/javascript");
        }
    };

    private final List<File> rootDirs;
    private final HttpHandler handler;

    public static class ServerRunner {
        public static void run(Class serverClass) {
            try {
                executeInstance((NanoHTTPD) serverClass.newInstance());
            } catch (Exception e) {
                e.printStackTrace();
            }
        }

        public static void executeInstance(NanoHTTPD server) {
            try {
                server.start();
            } catch (IOException ioe) {
                System.exit(-1);
            }

            try {
                System.in.read();
            } catch (Throwable ignored) {
            }
            server.stop();
        }
    }

    public FileServer(String host, int port, List<File> wwwroots, QueryHandler queryHandler) {
        super(host, port);
        this.rootDirs = new ArrayList<File>(wwwroots);
        this.handler = queryHandler;
    }

    private List<File> getRootDirs() {
        return rootDirs;
    }

    public Response serve(IHTTPSession session) {
        Map<String, String> header = session.getHeaders();
        String uri = session.getUri();

        for (File homeDir : getRootDirs()) {
            if (!homeDir.isDirectory()) {
                return getInternalErrorResponse("given path is not a directory (" + homeDir + ").");
            }
        }
        return respond(Collections.unmodifiableMap(header), session, uri);
    }

    private Response respond(Map<String, String> headers, IHTTPSession session, String uri) {
        uri = uri.trim().replace(File.separatorChar, '/');
        if (uri.indexOf('?') >= 0) {
            uri = uri.substring(0, uri.indexOf('?'));
        }

        if (uri.startsWith("src/main") || uri.endsWith("src/main") || uri.contains("../")) {
            return getForbiddenResponse("Won't serve ../ for security reasons.");
        }

        boolean canServeUri = false;
        File homeDir = null;
        List<File> roots = getRootDirs();
        for (int i = 0; !canServeUri && i < roots.size(); i++) {
            homeDir = roots.get(i);
            canServeUri = canServeUri(uri, homeDir);
        }
        if (!canServeUri) {
            HttpExchange fhe = new FakeHttpExchange(URI.create("http://" + headers.get("host")
                    + uri + "?" + session.getQueryParameterString()));
            for (String key : headers.keySet()) {
                List<String> slst = new ArrayList<String>();
                slst.add(headers.get(key));
                fhe.getRequestHeaders().put(key, slst);
            }
            try {
                handler.handle(fhe);
            } catch (IOException e) {
            }
            return createResponse(
                    Response.Status.OK,
                    NanoHTTPD.MIME_PLAINTEXT,
                    new ByteArrayInputStream(((ByteArrayOutputStream) fhe.getResponseBody())
                            .toByteArray()));
        }

        File f = new File(homeDir, uri);

        String mimeTypeForFile = getMimeTypeForFile(uri);
        Response response = null;
        response = serveFile(uri, headers, f, mimeTypeForFile);

        return response;
    }

    protected Response getNotFoundResponse() {
        return createResponse(Response.Status.NOT_FOUND, NanoHTTPD.MIME_PLAINTEXT,
                "Error 404, file not found.");
    }

    protected Response getForbiddenResponse(String s) {
        return createResponse(Response.Status.FORBIDDEN, NanoHTTPD.MIME_PLAINTEXT, "FORBIDDEN: "
                + s);
    }

    protected Response getInternalErrorResponse(String s) {
        return createResponse(Response.Status.INTERNAL_ERROR, NanoHTTPD.MIME_PLAINTEXT,
                "INTERNAL ERRROR: " + s);
    }

    private boolean canServeUri(String uri, File homeDir) {
        boolean canServeUri;
        File f = new File(homeDir, uri);
        canServeUri = f.exists();
        return canServeUri;
    }

    Response serveFile(String uri, Map<String, String> header, File file, String mime) {
        Response res;
        try {
            String etag = Integer
                    .toHexString((file.getAbsolutePath() + file.lastModified() + "" + file.length())
                            .hashCode());

            long fileLen = file.length();

            if (etag.equals(header.get("if-none-match")))
                res = createResponse(Response.Status.NOT_MODIFIED, mime, "");
            else {
                res = createResponse(Response.Status.OK, mime, new FileInputStream(file));
                res.addHeader("Content-Length", "" + fileLen);
                res.addHeader("ETag", etag);
            }
        } catch (IOException ioe) {
            res = getForbiddenResponse("Reading file failed.");
        }

        return res;
    }

    private String getMimeTypeForFile(String uri) {
        int dot = uri.lastIndexOf('.');
        String mime = null;
        if (dot >= 0) {
            mime = MIME_TYPES.get(uri.substring(dot + 1).toLowerCase());
        }
        return mime == null ? MIME_DEFAULT_BINARY : mime;
    }

    private Response createResponse(Response.Status status, String mimeType, InputStream message) {
        Response res = new Response(status, mimeType, message);
        res.addHeader("Accept-Ranges", "bytes");
        return res;
    }

    private Response createResponse(Response.Status status, String mimeType, String message) {
        Response res = new Response(status, mimeType, message);
        res.addHeader("Accept-Ranges", "bytes");
        return res;
    }
}
