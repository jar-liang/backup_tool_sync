package top.jarhub.backup.app.config;

import com.alibaba.fastjson2.JSONArray;
import com.alibaba.fastjson2.JSONObject;
import org.apache.commons.io.IOUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import top.jarhub.backup.app.pojo.User;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.Objects;

public final class ConfigUtils {
    private static final Logger LOG = LoggerFactory.getLogger(ConfigUtils.class);
    private static List<User> users = null;
    private static String filePath = null;
    private static String sourceDir = null;
    private static String targetDir = null;
    private static String processTipPath = null;
    private static String zipSavePath = null;
    private static String mongoDbSavePath = null;
    private static String executeTime = null;

    private ConfigUtils() {}

    public synchronized static String getExecuteTime() throws IOException {
        if (executeTime == null) {
            String configStrings = getConfigStrings();
            JSONObject jsonObject = JSONObject.parseObject(configStrings);
            executeTime = String.valueOf(Objects.requireNonNull(jsonObject.get("execute_time"), "execute_time not configured"));
        }
        return executeTime;
    }

    public synchronized static String getMongoDbSavePath() throws IOException {
        if (mongoDbSavePath == null) {
            String configStrings = getConfigStrings();
            JSONObject jsonObject = JSONObject.parseObject(configStrings);
            mongoDbSavePath = String.valueOf(Objects.requireNonNull(jsonObject.get("mongodb_backup_dir"), "mongodb_backup_dir not configured"));
        }
        return mongoDbSavePath;
    }

    public synchronized static String getZipSavePath() throws IOException {
        if (zipSavePath == null) {
            String configStrings = getConfigStrings();
            JSONObject jsonObject = JSONObject.parseObject(configStrings);
            zipSavePath = String.valueOf(Objects.requireNonNull(jsonObject.get("zip_dir"), "zip_dir not configured"));
        }
        return zipSavePath;
    }

    public synchronized static String getProcessTipPath() throws IOException {
        if (processTipPath == null) {
            String configStrings = getConfigStrings();
            JSONObject jsonObject = JSONObject.parseObject(configStrings);
            processTipPath = String.valueOf(Objects.requireNonNull(jsonObject.get("process_tip"), "process_tip not configured"));
        }
        return processTipPath;
    }

    public synchronized static String getTargetDir() throws IOException {
        if (targetDir == null) {
            String configStrings = getConfigStrings();
            JSONObject jsonObject = JSONObject.parseObject(configStrings);
            targetDir = String.valueOf(Objects.requireNonNull(jsonObject.get("target_dir"), "target_dir not configured"));
        }
        return targetDir;
    }

    public synchronized static String getSourceDir() throws IOException {
        if (sourceDir == null) {
            String configStrings = getConfigStrings();
            JSONObject jsonObject = JSONObject.parseObject(configStrings);
            sourceDir = String.valueOf(Objects.requireNonNull(jsonObject.get("source_dir"), "source_dir not configured"));
        }
        return sourceDir;
    }

    public synchronized static String getFilePath() throws IOException{
        if (filePath == null) {
            String configStrings = getConfigStrings();
            JSONObject jsonObject = JSONObject.parseObject(configStrings);
            Object filePathObj = Objects.requireNonNull(jsonObject.get("filePath"), "filePath not configured");
            filePath = String.valueOf(filePathObj);
        }
        return filePath;
    }

    public synchronized static List<User> getUsers() throws IOException {
        if (users == null) {
            String configStrings = getConfigStrings();
            JSONObject jsonObject = JSONObject.parseObject(configStrings);
            Object usersObj = jsonObject.get("users");
            if (usersObj instanceof JSONArray) {
                JSONArray jsonArray = (JSONArray) usersObj;
                users = jsonArray.toJavaList(User.class);
            } else {
                throw new IOException("user file not configure users");
            }
        }
        return users;
    }

    private static String getConfigStrings() throws IOException {
        File file = new File("config" + File.separator + "server_config.json");
        if (!file.exists()) {
            throw new IOException("user file not exist");
        }
        List<String> lines = IOUtils.readLines(new FileInputStream(file), StandardCharsets.UTF_8);
        StringBuilder builder = new StringBuilder();
        lines.forEach(builder::append);
        return builder.toString();
    }
}
