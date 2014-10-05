
package edu.nyu.cs.cs2580.utils;

import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.io.PrintWriter;
import java.io.BufferedWriter;
import java.sql.Timestamp;
import java.util.Date;

public class ClickLoggingManager {
    private static ClickLoggingManager instance = null;

    private static final String LOGGING_FILE = "./results/hw1.4-log.tsv";

    private PrintWriter out = null;

    private ClickLoggingManager() {
        try {
            out = new PrintWriter(
                    new BufferedWriter(new FileWriter(new File(LOGGING_FILE).getAbsolutePath(),
                            true)));
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    public static ClickLoggingManager getInstance() {
        if (instance == null) {
            instance = new ClickLoggingManager();
        }
        return instance;
    }

    public void writeToLog(String session, String query, String did, String action) {
        Date date = new Date();
        Timestamp ts = new Timestamp(date.getTime());
        out.println(session + "\t" + query + "\t" + did + "\t" + action + "\t" + ts);
        out.flush();
    }
}
