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

    private ConfigUtils() {}

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
