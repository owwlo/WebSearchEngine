package edu.nyu.cs.cs2580.utils;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.InetSocketAddress;
import java.net.URI;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import com.sun.net.httpserver.Authenticator;
import com.sun.net.httpserver.Filter;
import com.sun.net.httpserver.Headers;
import com.sun.net.httpserver.HttpContext;
import com.sun.net.httpserver.HttpExchange;
import com.sun.net.httpserver.HttpHandler;
import com.sun.net.httpserver.HttpPrincipal;
import com.sun.net.httpserver.HttpServer;

public class FakeHttpExchange extends HttpExchange {
    private Headers responseHeaders = new Headers();
    private Headers requestHeaders = new Headers();
    private String method = "GET";
    private URI uri = null;
    private OutputStream outputStream = new ByteArrayOutputStream();
    private Map<String, Object> map = new HashMap<String, Object>();

    public FakeHttpExchange(URI uri) {
        super();
        this.uri = uri;
    }
    
    @Override
    public void close() {
    }

    public Map<String, Object> getAttributes() {
        return map;
    }
    
    @Override
    public Object getAttribute(String arg0) {
        if (map.containsKey(arg0)) {
            return map.get(arg0);
        }
        return null;
    }

    @Override
    public HttpContext getHttpContext() {
        return null;
    }

    @Override
    public InetSocketAddress getLocalAddress() {
        return null;
    }

    @Override
    public HttpPrincipal getPrincipal() {
        return null;
    }

    @Override
    public String getProtocol() {
        return null;
    }

    @Override
    public InetSocketAddress getRemoteAddress() {
        return null;
    }

    @Override
    public InputStream getRequestBody() {
        return null;
    }

    @Override
    public Headers getRequestHeaders() {
        return requestHeaders;
    }

    @Override
    public String getRequestMethod() {
        return method;
    }

    @Override
    public URI getRequestURI() {
        return uri;
    }

    @Override
    public OutputStream getResponseBody() {
        return outputStream;
    }

    @Override
    public int getResponseCode() {
        return 0;
    }

    @Override
    public Headers getResponseHeaders() {
        return responseHeaders;
    }

    @Override
    public void sendResponseHeaders(int arg0, long arg1) throws IOException {

    }

    @Override
    public void setAttribute(String arg0, Object arg1) {
        map.put(arg0, arg1);
    }

    @Override
    public void setStreams(InputStream arg0, OutputStream arg1) {
    }

}
