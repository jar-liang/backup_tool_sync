package top.jarhub;

import com.alibaba.fastjson2.JSONArray;
import com.alibaba.fastjson2.JSONObject;
import org.apache.commons.configuration2.Configuration;
import org.apache.commons.configuration2.builder.fluent.Configurations;
import org.apache.commons.configuration2.ex.ConfigurationException;
import org.apache.hc.client5.http.classic.methods.HttpPost;
import org.apache.hc.client5.http.impl.classic.BasicHttpClientResponseHandler;
import org.apache.hc.client5.http.impl.classic.CloseableHttpClient;
import org.apache.hc.client5.http.impl.classic.HttpClientBuilder;
import org.apache.hc.client5.http.impl.classic.HttpClients;
import org.apache.hc.client5.http.impl.io.PoolingHttpClientConnectionManager;
import org.apache.hc.client5.http.impl.io.PoolingHttpClientConnectionManagerBuilder;
import org.apache.hc.client5.http.ssl.NoopHostnameVerifier;
import org.apache.hc.client5.http.ssl.SSLConnectionSocketFactory;
import org.apache.hc.client5.http.ssl.SSLConnectionSocketFactoryBuilder;
import org.apache.hc.client5.http.ssl.TrustAllStrategy;
import org.apache.hc.core5.http.ContentType;
import org.apache.hc.core5.http.Header;
import org.apache.hc.core5.http.HttpEntity;
import org.apache.hc.core5.http.io.HttpClientResponseHandler;
import org.apache.hc.core5.http.io.entity.StringEntity;
import org.apache.hc.core5.net.URIBuilder;
import org.apache.hc.core5.ssl.SSLContexts;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.net.ssl.SSLContext;
import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.Calendar;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

public class SyncClient {
    private static final Logger LOG = LoggerFactory.getLogger(SyncClient.class);

    public static void main(String[] args) {
        Configurations configurations = new Configurations();
        Configuration config;
        try {
            File file = new File("config" + File.separator + "sync-client.properties");
            config = configurations.properties(file);
            LOG.info("read config success");
        } catch (ConfigurationException e) {
            LOG.error("read config error: {}", e.getMessage());
            return;
        }

        String listPath = config.getString("list.path");
        String downloadPath = config.getString("download.path");
        if (listPath == null || downloadPath == null) {
            LOG.error("list.path or download.path not configure!");
            return;
        }
        URI listFilesUri = getUri(config, listPath);
        URI downloadUri = getUri(config, downloadPath);
        if (listFilesUri == null || downloadUri == null) {
            LOG.error("listFilesUri or downloadUri is null, app stop!");
            return;
        }

        ScheduledExecutorService executorService = Executors.newScheduledThreadPool(2);
        // 定时每天几点执行
        long secondGap = getSecondGap(config.getInt("hour", 4), config.getInt("minute", 0), config.getInt("second", 0));
        SyncClient syncClient = new SyncClient();
        executorService.scheduleAtFixedRate(() -> {
            try {
                syncClient.listFileAndDownload(listFilesUri, downloadUri, config);
            } catch (Exception e) {
                LOG.error("execute schedule task got exception, error message: {}", e.getMessage());
            }
        }, secondGap, 24 * 3600, TimeUnit.SECONDS);

        LOG.info("sync client started!");
    }

    private static long getSecondGap(int hour, int minute, int second) {
        Calendar schedule = Calendar.getInstance();
        schedule.set(Calendar.HOUR_OF_DAY, hour);
        schedule.set(Calendar.MINUTE, minute);
        schedule.set(Calendar.SECOND, second);

        Calendar now = Calendar.getInstance();

        if (now.compareTo(schedule) > 0) {
            schedule.add(Calendar.DAY_OF_MONTH, 1);
        }

        long scheduleTimeMillis = schedule.getTimeInMillis() / 1000;
        long nowTimeMillis = now.getTimeInMillis() / 1000;
        return scheduleTimeMillis - nowTimeMillis;
    }

    private void listFileAndDownload(URI listFilesUri, URI downloadUri, Configuration config) {
        try (CloseableHttpClient httpClient = HttpClients.createDefault()) {
            HttpPost httpPost = new HttpPost(listFilesUri);
            Map<String, String> entityMap = new HashMap<>();
            entityMap.put("userName", config.getString("userName"));
            entityMap.put("password", config.getString("password"));
            httpPost.setEntity(new StringEntity(getEntityStrings(entityMap), ContentType.APPLICATION_FORM_URLENCODED, "UTF-8", false));
            String result = httpClient.execute(httpPost, new BasicHttpClientResponseHandler());
            // 解析json返参
            JSONObject jsonObject = JSONObject.parseObject(result);
            Object status = jsonObject.get("status");
            if ("1".equals(status)) {
                JSONArray filesArray = jsonObject.getJSONArray("files");
                List<String> fileList = filesArray.toJavaList(String.class);
                String localSavePath = config.getString("localSavePath", "save");
                File savePath = new File(localSavePath);
                boolean isPathExist = savePath.exists();
                if (!isPathExist) {
                    isPathExist = savePath.mkdirs();
                    if (!isPathExist) {
                        throw new IOException("local save path not exist and make directory failed");
                    }
                }

                for (String fileName : fileList) {
                    File file = new File(localSavePath + File.separator + fileName);
                    if (file.exists()) {
                        LOG.info("file exist: {}", fileName);
                        continue;
                    }
                    downloadFile(downloadUri, config, fileName);
                }
            } else {
                LOG.warn(String.valueOf(jsonObject.get("msg")));
            }
        } catch (Exception e) {
            LOG.error("http client error: " + e.getMessage());
        }
    }

    private void downloadFile(URI uri, Configuration config, String fileName) {
        try (CloseableHttpClient httpClient = HttpClients.createDefault()) {
            HttpPost httpPost = new HttpPost(uri);
            Map<String, String> entityMap = new HashMap<>();
            entityMap.put("userName", config.getString("userName"));
            entityMap.put("password", config.getString("password"));
            entityMap.put("fileName", fileName);
            String entityStrings = getEntityStrings(entityMap);
            httpPost.setEntity(new StringEntity(entityStrings, ContentType.APPLICATION_FORM_URLENCODED, "UTF-8", false));
            String localSavePath = config.getString("localSavePath", "save");
            httpClient.execute(httpPost, buildHttpClientResponseHandler(localSavePath));
        } catch (Exception e) {
            LOG.error("http client error: " + e.getMessage());
        }
    }

    private String getEntityStrings(Map<String, String> entityMap) {
        StringBuilder builder = new StringBuilder();
        entityMap.forEach((k, v) -> builder.append(k).append("=").append(v).append("&"));
        builder.deleteCharAt(builder.lastIndexOf("&"));
        return builder.toString();
    }

    private HttpClientResponseHandler<HttpEntity> buildHttpClientResponseHandler(String localSavePath) {
        return classicHttpResponse -> {
            HttpEntity entity = classicHttpResponse.getEntity();
            Header header = classicHttpResponse.getHeader("Content-Disposition");
            boolean isDownload = false;
            String fileName = "";
            if (header != null && header.getValue() != null) {
                String value = header.getValue();
                String[] split = value.split("=");
                if (split.length > 1 && split[1] != null) {
                    fileName = split[1];
                    File path = new File(localSavePath);
                    boolean isPathExist = true;
                    if (!path.exists()) {
                        isPathExist = path.mkdirs();
                    }
                    if (isPathExist) {
                        File file = new File(localSavePath + File.separator + split[1]);
                        try (OutputStream outputStream = new FileOutputStream(file)) {
                            entity.writeTo(outputStream);
                            outputStream.flush();
                            isDownload = true;
                        }
                    }
                }
            }
            if (isDownload) {
                LOG.info("download success, file name is: {}", fileName);
            } else {
                LOG.warn("download failed, file name is: {}", fileName);
            }
            return entity;
        };
    }

    private static URI getUri(Configuration config, String path) {
        boolean isHttps = config.getBoolean("isHttps", false);
        String host = config.getString("host", "localhost");
        int port;
        URI uri = null;
        try {
            if (isHttps) {
                port = config.getInt("port", 443);
                uri = new URIBuilder().setScheme("https").setHost(host).setPort(port).setPath(path).build();
            } else {
                port = config.getInt("port", 80);
                uri = new URIBuilder().setScheme("http").setHost(host).setPort(port).setPath(path).build();
            }
        } catch (URISyntaxException e) {
            LOG.error("build url error: {}", e.getMessage());
        }
        return uri;
    }

    /**
     * 生成不验证证书的httpClient
     *
     * @return CloseableHttpClient
     * @throws Exception
     */
    public CloseableHttpClient buildHttpClient() throws Exception {
        TrustAllStrategy trustAllStrategy = new TrustAllStrategy();
        SSLContext sslContext = SSLContexts.custom().loadTrustMaterial(null, trustAllStrategy).build();
        SSLConnectionSocketFactory socketFactory = new SSLConnectionSocketFactoryBuilder().setSslContext(sslContext)
                .setHostnameVerifier(new NoopHostnameVerifier()).build();
        PoolingHttpClientConnectionManager connectionManager = PoolingHttpClientConnectionManagerBuilder.create().setSSLSocketFactory(socketFactory).build();
        HttpClientBuilder httpClientBuilder = HttpClientBuilder.create();
        httpClientBuilder.setConnectionManager(connectionManager);
        return httpClientBuilder.build();
    }
}
