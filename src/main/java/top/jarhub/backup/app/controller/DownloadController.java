package top.jarhub.backup.app.controller;

import com.alibaba.fastjson2.JSONArray;
import com.alibaba.fastjson2.JSONObject;
import org.apache.tomcat.util.http.fileupload.IOUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Controller;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.ResponseBody;
import top.jarhub.backup.app.config.ConfigUtils;

import javax.servlet.http.HttpServletResponse;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.URLEncoder;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;

@Controller
@RequestMapping("sync")
public class DownloadController {
    private static final Logger LOGGER = LoggerFactory.getLogger(DownloadController.class);

    @ResponseBody
    @RequestMapping("test")
    public String test() {
        LocalDateTime now = LocalDateTime.now();
        String nowString = now.format(DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss"));
        LOGGER.info("request test at: {}", nowString);
        return "server time: " + nowString;
    }

    @ResponseBody
    @RequestMapping("list")
    public String listFiles() {
        try {
            File file = new File(ConfigUtils.getFilePath());
            if (!file.exists()) {
                throw new IOException("dir not exist");
            }
            File[] files = file.listFiles();
            if (files == null) {
                throw new IOException("list files null");
            }
            JSONObject jsonObject = JSONObject.of("status", "1");
            JSONArray jsonArray = new JSONArray();
            for(File item : files) {
                if (!item.isDirectory()) {
                    jsonArray.add(item.getName());
                }
            }
            jsonObject.put("files", jsonArray);
            return jsonObject.toString();
        } catch (Exception e) {
            LOGGER.error("[listFiles]: list files error: {}", e.getMessage());
            JSONObject jsonObject = JSONObject.of("status", "0");
            jsonObject.put("msg", "list files error");
            return jsonObject.toString();
        }
    }

    @ResponseBody
    @RequestMapping("download")
    public String fileDownload(String fileName, HttpServletResponse response) {
        try {
            File file = new File(ConfigUtils.getFilePath() + File.separator + fileName);
            File canonicalFile = file.getCanonicalFile();
            if (!canonicalFile.exists()) {
                return "File not exist, download fail";
            }
            try (InputStream inputStream = new FileInputStream(canonicalFile)) {
                int available = inputStream.available();
                if (available > 1073741824) { // 1024 * 1024 * 1024 byte = 1GB
                    LOGGER.warn("[fileDownload]: large file size: " + available + "byte");
                    return "File too large, download fail";
                }
                OutputStream outputStream = response.getOutputStream();
                response.setContentLength(available);
                response.setContentType("application/octet-stream");
                String encodeFileName = URLEncoder.encode(canonicalFile.getName(), "UTF-8");
                response.setHeader("Content-Disposition", "attachment;filename=" + encodeFileName);
                IOUtils.copy(inputStream, outputStream);
                outputStream.flush();
            }
            return null;
        } catch (Exception e) {
            LOGGER.error("[fileDownload]: business error, download fail. detail: " + e.getMessage());
            return "business error, download fail";
        }
    }
}
