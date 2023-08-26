package top.jarhub.backup.app.util;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.util.zip.ZipEntry;
import java.util.zip.ZipOutputStream;

public final class FileDealUtils {
    private static final Logger LOG = LoggerFactory.getLogger(FileDealUtils.class);
    private FileDealUtils() {
    }

    /**
     * 文件压缩方法
     * @param sourcePath 要压缩的目录或文件
     * @param targetFile 生成压缩文件的路径(包含压缩文件名)
     * @return 压缩成功与否
     */
    public static boolean compressFiles(String sourcePath, String targetFile) {
        File sourceFile = new File(sourcePath);
        if (!sourceFile.exists()) {
            LOG.error("source file [" + sourcePath + "] not exist, cannot compress");
            return false;
        }

        File target = new File(targetFile);
        if (target.exists()) {
            LOG.error("target file [" + targetFile + "] exist, compress interrupt, please check");
            return false;
        }

        File parentFile = target.getParentFile();
        if (!parentFile.exists()) {
            LOG.error("target file's parent dir not exist.");
            return false;
        }

        try (ZipOutputStream zos = new ZipOutputStream(new FileOutputStream(targetFile))) {
            zipFiles(zos, sourceFile, sourceFile.getName());
            return true;
        } catch (IOException e) {
            LOG.error("compress file failed! error message: " + e.getMessage());
        }
        return false;
    }

    private static void zipFiles(ZipOutputStream zos, File sourceFile, String zipEntryName) throws IOException {
        if (sourceFile.isDirectory()) {
            zos.putNextEntry(new ZipEntry(zipEntryName + File.separator));
            zos.closeEntry();

            File[] files = sourceFile.listFiles();
            if (files != null && files.length > 0) {
                for (File file : files) {
                    String subZipEntryName = zipEntryName + File.separator + file.getName();
                    zipFiles(zos, file, subZipEntryName);
                }
            }

            return;
        }

        zos.putNextEntry(new ZipEntry(zipEntryName));
        try (FileInputStream fis = new FileInputStream(sourceFile)) {
            byte[] buf = new byte[2 * 1024];
            while (true) {
                int length = fis.read(buf);
                if (length < 0) {
                    break;
                }
                zos.write(buf, 0, length);
            }
        }

        zos.closeEntry();
    }
}
