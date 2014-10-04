
package edu.nyu.cs.cs2580;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import edu.nyu.cs.cs2580.nanohttpd.FileServer;
import edu.nyu.cs.cs2580.nanohttpd.FileServer.ServerRunner;

public class SearchEngine {
    // @CS2580: please use a port number 258XX, where XX corresponds
    // to your group number.
    public static void main(String[] args) throws IOException {
        // Create the server.
        if (args.length < 2) {
            System.out.println("arguments for this program are: [PORT] [PATH-TO-CORPUS]");
            return;
        }
        int port = Integer.parseInt(args[0]);

        String index_path = args[1];
        Ranker ranker = new Ranker(index_path);

        List<File> rootDirs = new ArrayList<File>();
        if (rootDirs.isEmpty()) {
            rootDirs.add(new File("./public/.").getAbsoluteFile());
        }
        ServerRunner.executeInstance(new FileServer("", port, rootDirs, new QueryHandler(ranker)));

        System.out.println("Listening on port: " + Integer.toString(port));
    }
}
