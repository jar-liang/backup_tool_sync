package top.jarhub.backup.app;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import top.jarhub.backup.app.backup.BackupData;
import top.jarhub.backup.app.config.ConfigUtils;

import java.util.Calendar;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

@SpringBootApplication
public class SpringbootBackupApplication {
	private static final Logger LOG = LoggerFactory.getLogger(SpringbootBackupApplication.class);

	public static void main(String[] args) {
		SpringApplication.run(SpringbootBackupApplication.class, args);
		runBackupData();
	}

	private static void runBackupData() {
		BackupData backupData = new BackupData();
		ScheduledExecutorService executorService = Executors.newScheduledThreadPool(2);
		// 定时每天几点执行，暂定凌晨3点
		long secondGap = getSecondGap();
		executorService.scheduleAtFixedRate(() -> {
			try {
				backupData.run();
			} catch (Exception e) {
				LOG.error("run backup data task failed: {}", e.getMessage());
			}
		},  secondGap, 24 * 3600, TimeUnit.SECONDS);
		LOG.info("running backup data! first task will execute in {} seconds", secondGap);
	}

	private static long getSecondGap() {
		int hour = 3;
		int minute = 0;
		int second = 0;
		try {
			String executeTime = ConfigUtils.getExecuteTime();
			String[] split = executeTime.split(":");
			if (split.length > 2) {
				hour = Integer.parseInt(split[0]);
				minute = Integer.parseInt(split[1]);
				second = Integer.parseInt(split[2]);
			}
		} catch (Exception e) {
			LOG.error("get execute time failed: " + e.getMessage());
		}
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

}
