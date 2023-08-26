package top.jarhub.backup.app.backup;

import org.apache.commons.io.IOUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import top.jarhub.backup.app.config.ConfigUtils;
import top.jarhub.backup.app.util.BackupMongoDB;
import top.jarhub.backup.app.util.FileDealUtils;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.OutputStreamWriter;
import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Calendar;
import java.util.Date;
import java.util.List;
import java.util.stream.Collectors;

public class BackupData {

    private static final Logger LOG =  LoggerFactory.getLogger(BackupData.class);

    private boolean backupMongoDBData(String mongoSavePath) {
        String osName = System.getProperty("os.name");
        if (osName != null && osName.contains("Linux")) {
            File file = new File(mongoSavePath);
            if (!file.exists()) {
                boolean mkdirs = file.mkdirs();
                if (!mkdirs) {
                    LOG.error("mongodb backup mkdirs: [" + mongoSavePath + "] failed!");
                    return false;
                }
            }
            if (file.exists() && file.isDirectory()) {
                boolean doBackup = BackupMongoDB.doBackup(mongoSavePath);
                if (!doBackup) {
                    LOG.error("backup mongodb failed!");
                }
               return doBackup;
            }
        }
        return false;
    }

    public void run() throws IOException {
        LOG.info("backup task starting ...");

        // 3.备案过程，遍历目录，已存在的不备份，不存在的备案，已删除的不理会
        String sourcePath = ConfigUtils.getSourceDir();
        File sourceDir = new File(sourcePath);
        if (!sourceDir.exists() || !sourceDir.isDirectory()) {
            LOG.error("source directory not exist or it is not a directory!");
            return;
        }

        String targetPath = ConfigUtils.getTargetDir();
        File targetDir = new File(targetPath);
        if (targetDir.exists()) {
            if (!targetDir.isDirectory()) {
                LOG.error("target path is not a directory!");
                return;
            }
        } else {
            boolean mkdirs = targetDir.mkdirs();
            if (!mkdirs) {
                LOG.error("make target directory fail!");
                return;
            }
        }

        String processPath = ConfigUtils.getProcessTipPath();
        File processFilePath = new File(processPath);
        if (!processFilePath.exists()) {
            boolean mkdirs = processFilePath.mkdirs();
            if (!mkdirs) {
                LOG.error("process tips save dir make failed");
                return;
            }
        }

        String zipSavePath = ConfigUtils.getZipSavePath();
        File zipDir = new File(zipSavePath);
        if (!zipDir.exists()) {
            boolean mkdirs = zipDir.mkdirs();
            if (!mkdirs) {
                LOG.error("zip file save dir make failed");
                return;
            }
        }

        String mongoSavePath = ConfigUtils.getMongoDbSavePath();

        File[] files = sourceDir.listFiles();
        // record记录备份成功和失败统计数据，第一个元素是成功n条，第二个元素是失败m条
        int[] record = {0, 0};
        StringBuilder tips = new StringBuilder();
        if (files != null && files.length > 0) {
            copyFiles(files, tips, record);
        }
        tips.append(getCurrentTimeFormat()).append("--[finish]--backup file finish, result: success[").append(record[0])
                .append("], fail[").append(record[1]).append("]").append("\n");

        // 4.执行shell命令，导出mongodb的数据，备份数据到存在的位置
        boolean backupMongoDBData = backupMongoDBData(mongoSavePath);
        if (backupMongoDBData) {
            tips.append(getCurrentTimeFormat()).append("--[backupDB]--success\n");
        } else {
            tips.append(getCurrentTimeFormat()).append("--[backupDB]--fail, detail in log file\n");
        }

        // 6.压缩备份文件，命名添加时间
        String zipFileName = "pic_backup_" + getCurrentTimeFormat("yyyyMMddHHmmss") + ".zip";
        String targetZipFile = zipSavePath + File.separator + zipFileName;
        boolean result = FileDealUtils.compressFiles(targetPath, targetZipFile);
        if (result) {
            tips.append(getCurrentTimeFormat()).append("--[zip]--pic data, success, zip file path: ").append(targetZipFile).append("\n");
        } else {
            tips.append(getCurrentTimeFormat()).append("--[zip]--pic data, fail, method return fail, detail in log file.").append("\n");
        }

        String mongodbFileName = "mongodb_backup_" + getCurrentTimeFormat("yyyyMMddHHmmss") + ".zip";
        String mongodbZipFile = zipSavePath + File.separator + mongodbFileName;
        result = FileDealUtils.compressFiles(mongoSavePath, mongodbZipFile);
        if (result) {
            tips.append(getCurrentTimeFormat()).append("--[zip]--mongodb data, success, zip file path: ").append(mongodbZipFile).append("\n");
        } else {
            tips.append(getCurrentTimeFormat()).append("--[zip]--mongodb data, fail, method return fail, detail in log file.").append("\n");
        }

        writeProcessFile(processPath, tips);
        // 6.删除n久之前的备份文件和过程记录文件
        // md5摘要判断，取最新的一份跟之前的对比，一致的除了是每月1号和15号的之外就删除。另外如果是不一样但是是最新文件的5天前的文件，也删除
        deleteHistoryFiles(zipSavePath, "pic_backup_");
        deleteHistoryFiles(zipSavePath, "mongodb_backup_");
        deleteHistoryFiles(processPath, "process_detail_");
        // 5.要不要远程复制备份数据，看情况吧
    }

    private void deleteHistoryFiles(String path, String filterKeyWord) {
        File file = new File(path);
        if (file.isDirectory()) {
            File[] zipFiles = file.listFiles((dir, name) -> name.contains(filterKeyWord));
            if (zipFiles == null || zipFiles.length < 1) {
                return;
            }
            List<File> orderedFiles = Arrays.stream(zipFiles).sorted((o1, o2) -> {
                String o1TimeStr = o1.getName().substring(filterKeyWord.length(), o1.getName().lastIndexOf("."));
                String o2TimeStr = o2.getName().substring(filterKeyWord.length(), o2.getName().lastIndexOf("."));
                return -o1TimeStr.compareTo(o2TimeStr);// 取负值，获取倒序
            }).collect(Collectors.toList());
            if (orderedFiles.size() <= 1) {
                return;
            }
            File newestFile = orderedFiles.get(0);
            byte[] newestFileMD5 = getMD5DigestBytes(newestFile);
            String newestFileName = newestFile.getName();
            String newestFileDateStr = newestFileName.substring(filterKeyWord.length(), filterKeyWord.length() + 8);
            Date last5Day;
            try {
                Date newestFileDate = new SimpleDateFormat("yyyyMMdd").parse(newestFileDateStr);
                Calendar calendar = Calendar.getInstance();
                calendar.setTime(newestFileDate);
                calendar.add(Calendar.DAY_OF_MONTH, -5);
                last5Day = calendar.getTime();
            } catch (Exception e) {
                LOG.error("get last 5 day, transfer date error! detail: " + e.getMessage());
                return;
            }
            orderedFiles.remove(newestFile);
            List<File> deleteList = new ArrayList<>();
            for (File item : orderedFiles) {
                String dayStr = item.getName().substring(filterKeyWord.length() + 6, filterKeyWord.length() + 6 + 2);
                if ("01".equals(dayStr) || "15".equals(dayStr)) {
                    continue;
                }
                String fileDayStr = item.getName().substring(filterKeyWord.length(), filterKeyWord.length() + 8);
                try {
                    Date fileDay = new SimpleDateFormat("yyyyMMdd").parse(fileDayStr);
                    if (fileDay.before(last5Day)) {
                        deleteList.add(item);
                        continue;
                    }
                } catch (ParseException e) {
                    LOG.error("transfer date error! detail: " + e.getMessage());
                    continue;
                }
                byte[] md5DigestBytes = getMD5DigestBytes(item);
                if (Arrays.equals(newestFileMD5, md5DigestBytes)) {
                    deleteList.add(item);
                }
            }
            if (deleteList.isEmpty()) {
                return;
            }
            for (File deleteItem : deleteList) {
                boolean result = deleteItem.delete();
                if (!result) {
                    LOG.error("file cannot be deleted! file: " + deleteItem.getName());
                }
            }
        }
    }

    private byte[] getMD5DigestBytes(File newestFile) {
        byte[] digest = null;
        try (FileInputStream fileInputStream = new FileInputStream(newestFile)) {
            MessageDigest md5 = MessageDigest.getInstance("MD5");
            byte[] data = new byte[4096];
            while (true) {
                int readLength = fileInputStream.read(data);
                if (readLength <= 0) {
                    break;
                }
                md5.update(data, 0, readLength);
            }
            digest = md5.digest();
        } catch (Exception e) {
            LOG.error("md5 check file error! detail: " + e.getMessage());
        }
        return digest;
    }

    private void writeProcessFile(String processPath, StringBuilder tips) {
        File processDetailFile = new File(processPath + File.separator + "process_detail_" + getCurrentTimeFormat("yyyyMMddHHmmss") + ".txt");
        if (!processDetailFile.exists()) {
            try {
                boolean newFile = processDetailFile.createNewFile();
                if (!newFile) {
                    LOG.error("create process detail file failed!");
                    return;
                }
            } catch (IOException e) {
                LOG.error("create process detail file exception, error message: " + e.getMessage());
                return;
            }
        }
        try (BufferedWriter bufferedWriter = new BufferedWriter(new OutputStreamWriter(new FileOutputStream(processDetailFile), StandardCharsets.UTF_8))) {
            bufferedWriter.write(tips.toString());
        } catch (IOException e) {
            LOG.error("save process detail file error: " + e.getMessage());
        }
    }

    private void copyFiles(File[] files, StringBuilder tips, int[] record) {
        if (files == null || files.length <= 0) {
            return;
        }
        for (File file : files) {
            try {
                String canonicalPath = file.getCanonicalPath();

                File sourceDir = new File(ConfigUtils.getSourceDir());
                String sourceDirCanonicalPath = sourceDir.getCanonicalPath();
                String tempPath = canonicalPath.substring(sourceDirCanonicalPath.length() + 1);
                File targetDir = new File(ConfigUtils.getTargetDir());
                String targetFilePath = targetDir.getCanonicalPath() + File.separator + tempPath;
                File targetFile = new File(targetFilePath);
                if (targetFile.exists() && !targetFile.isDirectory()) {
                    continue;
                }
                if (file.isDirectory()) {
                    if (!targetFile.exists()) {
                        boolean mkdirs = targetFile.mkdirs();
                        if (!mkdirs) {
                            String mkdirFail = "make target dir failed, target path: " + targetFilePath;
                            LOG.error(mkdirFail);
                            String currentTime = getCurrentTimeFormat();
                            tips.append(currentTime).append("--[FAIL]--").append(mkdirFail).append("\n");
                        }
                    }
                    File[] listFiles = file.listFiles();
                    copyFiles(listFiles, tips, record);
                } else {
                    File parentFile = targetFile.getParentFile();
                    if (!parentFile.isDirectory() || !parentFile.exists()) {
                        String parentFileFail = targetFilePath + ", it's parent file path not exist or not a directory";
                        LOG.error(parentFileFail);
                        tips.append(getCurrentTimeFormat()).append("--[FAIL]--").append(parentFileFail).append("\n");
                        record[1]++;
                        continue;
                    }
                    long length = file.length();
                    if (length > Integer.MAX_VALUE) {
                        String sourceFileTooLarge = "file size larger than 2GB, file path: " + canonicalPath;
                        LOG.error(sourceFileTooLarge);
                        tips.append(getCurrentTimeFormat()).append("--[FAIL]--").append(sourceFileTooLarge).append("\n");
                        record[1]++;
                        continue;
                    }
                    IOUtils.copyLarge(new FileInputStream(file), new FileOutputStream(targetFile));
                    record[0]++;
                    tips.append(getCurrentTimeFormat()).append("--[SUCCESS]--").append(canonicalPath).append("\n");
                }
            } catch (Exception e) {
                String exceptionMsg = "do file backup throw exception. current path is: " + (file.isDirectory() ? "directory" : "file")
                        + ". path name: " + file.getName() + ". error message: " + e.getMessage();
                LOG.error(exceptionMsg);
                if (!file.isDirectory()) {
                    record[1]++;
                }
                tips.append(getCurrentTimeFormat()).append("--[EXCEPTION]--").append(exceptionMsg).append("\n");
            }
        }
    }

    private String getCurrentTimeFormat() {
        SimpleDateFormat simpleDateFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
        return simpleDateFormat.format(new Date());
    }

    private String getCurrentTimeFormat(String formatPatter) {
        SimpleDateFormat simpleDateFormat = new SimpleDateFormat(formatPatter);
        return simpleDateFormat.format(new Date());
    }
}
