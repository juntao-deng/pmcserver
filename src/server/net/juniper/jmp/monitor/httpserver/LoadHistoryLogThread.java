package net.juniper.jmp.monitor.httpserver;

import java.io.File;

import net.juniper.jmp.monitor.extractor.ThreadExtractor;
import net.juniper.jmp.monitor.persist.LogPersister;
import net.juniper.jmp.tracer.common.MonitorConstants;
import net.juniper.jmp.tracer.dumper.info.ThreadInfoDump;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class LoadHistoryLogThread implements Runnable {
	private Logger logger = LoggerFactory.getLogger(LoadHistoryLogThread.class);
	public LoadHistoryLogThread() {
	}
	@Override
	public void run() {
		File dir = new File(MonitorConstants.SHARE_BASE_DIR + "pmclog");
		if(!dir.exists())
			dir.mkdirs();
		LogPersister.getInstance().tryCreateTables();
		while(true){
			try {
				String version = LogPersister.getInstance().getLastUpdateTime();
				if(version == null || version.trim().equals("")){
					version = "2013-01-01-00-00";
				}
				fetchLogs(version);
				Thread.sleep(2 * 60 * 1000);
			} 
			catch (InterruptedException e) {
				logger.error(e.getMessage(), e);
			}
		}
	}
	private void fetchLogs(String version) {
		Object[] results = ThreadExtractor.getInstance().getThreadInfos(version);
		while(results != null){
			LogPersister.getInstance().persistLog((ThreadInfoDump[])results[1], (String) results[0]);			
			if(((Boolean)results[2]).booleanValue()){
				results = ThreadExtractor.getInstance().getThreadInfos((String) results[0]);
			}
			else
				break;
		}
		ThreadExtractor.getInstance().deleteLastHourLogs();
	}
}
