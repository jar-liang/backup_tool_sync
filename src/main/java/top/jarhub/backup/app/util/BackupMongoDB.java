package top.jarhub.backup.app.util;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;

public class BackupMongoDB {
    private static final Logger LOG = LoggerFactory.getLogger(BackupMongoDB.class);

    public static boolean doBackup(String dumpPath) {
        String rmDir = dumpPath + File.separator + "leanote";
        String[] rmCmd = {"sh", "-c", "rm -rf " + rmDir};
        execCmd(rmCmd);

        String[] dumpDataCmd = new String[]{"sh", "-c", "mongodump -h localhost -d leanote -o " + dumpPath};
        int res = execCmd(dumpDataCmd);
        return res == 0;
    }

    private static int execCmd(String[] cmdString) {
        Process process = null;
        try {
            process = Runtime.getRuntime().exec(cmdString);
            return process.waitFor();
        } catch (Exception e) {
            LOG.error("execute cmd [" + cmdString[2] + "] got exception, error message: " + e.getMessage());
        } finally {
            if (process != null) {
                process.destroy();
            }
        }
        return -1;
    }
}
