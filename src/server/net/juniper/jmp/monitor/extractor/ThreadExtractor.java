package net.juniper.jmp.monitor.extractor;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.FilenameFilter;
import java.io.IOException;
import java.io.InputStreamReader;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.Collections;
import java.util.Date;
import java.util.List;

import net.juniper.jmp.tracer.common.MonitorConstants;
import net.juniper.jmp.tracer.dumper.info.MethodInfoDump;
import net.juniper.jmp.tracer.dumper.info.SqlInfoDump;
import net.juniper.jmp.tracer.dumper.info.StageInfoBaseDump;
import net.juniper.jmp.tracer.dumper.info.ThreadInfoDump;

import org.apache.log4j.Logger;

public final class ThreadExtractor {
	private Logger logger = Logger.getLogger(ThreadExtractor.class);
	private static ThreadExtractor instance = new ThreadExtractor();
	private static String baseDir = MonitorConstants.SHARE_BASE_DIR + "/pmclog";
	
	public static ThreadExtractor getInstance() {
		return instance;
	}
	
	public Object[] getThreadInfos(final String fromTs){
		File dir = new File(baseDir);
		if(!dir.exists()){
			logger.error("Log directories can not be found:" + baseDir);
			return null;
		}
		
		final List<String> suffixList = new ArrayList<String>();
		String[] fsstrs = dir.list(new FilenameFilter(){
			@Override
			public boolean accept(File dir, String name) {
				if(name.equals(MonitorConstants.PMC_THREAD_LOG))
					return false;
				if(name.startsWith(MonitorConstants.PMC_THREAD_LOG)){
					String suffix = name.substring(MonitorConstants.PMC_THREAD_LOG.length() + 1);
					boolean later = suffix.compareTo(fromTs) > 0;
					if(later){
						suffixList.add(suffix);
					}
					return later;
				}
				return false;
			}
		});
		//no log files, return null
		if(fsstrs == null || fsstrs.length == 0)
			return null;
		
		Object[] tripple = new Object[3];
		
		Collections.sort(suffixList);
		boolean reserved = false;
		int reserverCount = 10;
		//only process 10 files one time. or maybe out of memory
		if(suffixList.size() > reserverCount){
			reserved = true;
			logger.info("---------------------:reserved, fromTs: " + fromTs + ", size: " + suffixList.size());
			while(suffixList.size() > reserverCount)
				suffixList.remove(reserverCount);
		}
		
		String[] suffixs = suffixList.toArray(new String[0]);
		String latestTs = suffixs[suffixs.length - 1];
		
		tripple[0] = latestTs;
		
		List<ThreadInfoDump> threads = extractAll(baseDir, suffixs, false);
		
		tripple[1] = threads.toArray(new ThreadInfoDump[0]);
		
		tripple[2] = reserved;
		return tripple;
	}

	private List<ThreadInfoDump> extractAll(String baseDir, String[] sufs, boolean delete) {
		List<ThreadInfoDump> threads = readThreadsFromFiles(baseDir, sufs, delete);
		List<StageInfoBaseDump> stages = readStagesFromFiles(baseDir, sufs, delete);
		List<SqlInfoDump> sqls = readSqlsFromFiles(baseDir, sufs, delete);
		List<MethodInfoDump> methods = readMethodsFromFiles(baseDir, sufs, delete);
		ExtractorHelper.mergeAll(threads, stages, sqls, methods);
		return threads;
	}
	
	public List<ThreadInfoDump> extractAll(String baseDir, String tfile, String suffix, boolean delete) {
		return extractAll(baseDir, new String[]{suffix}, delete);
	}

	private List<MethodInfoDump> readMethodsFromFiles(String baseDir, String[] sufs, boolean delete) {
		return new MethodInfoExtractor(baseDir, sufs, delete).extract();
	}

	private List<SqlInfoDump> readSqlsFromFiles(String baseDir, String[] sufs, boolean delete) {
		return new SqlInfoExtractor(baseDir, sufs, delete).extract();
	}

	private List<StageInfoBaseDump> readStagesFromFiles(String baseDir, String[] sufs, boolean delete) {
		return new StageExtractor(baseDir, sufs, delete).extract();
	}

	private List<ThreadInfoDump> readThreadsFromFiles(String baseDir, String[] sufs, boolean delete) {
		List<ThreadInfoDump> threadList = new ArrayList<ThreadInfoDump>();
		for(String suf : sufs){
			String fileName = MonitorConstants.PMC_THREAD_LOG + "." + suf;
			File f = new File(baseDir + "/" + fileName);
			if(f.exists()){
				BufferedReader reader = null;
				try{
					reader = new BufferedReader(new InputStreamReader(new FileInputStream(f)));
					String line = reader.readLine();
						
					while(line != null){
						ThreadInfoDump t = ThreadInfoDump.fromString(line);
						if(t != null)
							threadList.add(t);
						line = reader.readLine();
					}
				}
				catch(Exception e){
					logger.error(e.getMessage(), e);
				}
				finally{
					if(reader != null){
						try {
							reader.close();
						} catch (IOException e) {
							logger.error(e.getMessage(), e);
						}
					}
				}
				if(delete)
					f.delete();
			}
		}
		return threadList;
	}
	
//	private String[] positionLogs(String baseDir, String[] fsstrs, String startTs, String endTs) {
//		List<String> fslist = new ArrayList<String>();
//		for(int i = 0; i < fsstrs.length; i ++){
//			String[] pair = getStartAndEndTs(baseDir + "/" + fsstrs[i]);
//			if(pair == null){
//				continue;
//			}
//			String fstartTs = pair[0];
//			String fendTs = pair[1];
//			//later than this file end
//			if(startTs.compareTo(fendTs) > 0)
//				break;
//			
//		}
//		return null;
//	}

//	private String[] getStartAndEndTs(String file) {
//		File f = new File(file);
//		RandomAccessFile rf = null;
//		try{
//			rf = new RandomAccessFile(f, "r");
//			String startTsLine = rf.readLine();
//			if(startTsLine == null)
//				return null;
//			long flength = rf.length();
//			if(flength > 2048)
//				rf.seek(rf.length() - 1024);
//			String endTsLine = startTsLine;
//			String endLine = rf.readLine();
//			while(endLine != null){
//				endTsLine = endLine;
//				endLine = rf.readLine();
//			}
//			
//			String startTs = getTsFromLine(startTsLine);
//			if(startTs == null)
//				return null;
//			String endTs = getTsFromLine(endTsLine);
//			if(endTs == null)
//				endTs = startTs;
//			return new String[]{startTs, endTs};
//		} 
//		catch (Exception e) {
//			logger.error(e.getMessage(), e);
//		}
//		finally{
//			if(rf != null){
//				try {
//					rf.close();
//				} 
//				catch (IOException e) {
//					logger.error(e.getMessage(), e);
//				}
//			}
//		}
//		return null;
//	}
	
//	private String getTsFromLine(String startTsLine) {
//		return null;
//	}

//	private MethodInfoDump extractMethod(File dir, String callId) {
//		return null;
//	}
//
//	private SqlInfoDump[] extractSqls(File dir, String callId) {
//		return null;
//	}
//
//	private StageInfoBaseDump[] extractStages(File dir, String callId) {
//		return null;
//	}


	public static void main(String[] args){
		System.out.println(System.getProperty("user.dir"));
	}

	public void deleteLastHourLogs() {
		Date d = Calendar.getInstance().getTime();
		SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd-hh");
		String suffix = sdf.format(d);
		suffix += "-" + "00"; 
		logger.info("deleting log before:" + suffix);
		String baseDir = MonitorConstants.SHARE_BASE_DIR + "pmclog";
		File dir = new File(baseDir);
		String threadLog = MonitorConstants.PMC_THREAD_LOG + "." + suffix;
		String sqlLog = MonitorConstants.PMC_SQL_LOG + "." + suffix;
		String stageLog = MonitorConstants.PMC_STAGE_LOG + "." + suffix;
		String methodLog = MonitorConstants.PMC_METHOD_LOG + "." + suffix;
		if(dir.exists()){
			File[] fs = dir.listFiles();
			for(int i = 0; i < fs.length; i ++){
				try{
					File f = fs[i];
					String fName = f.getName();
					if(!fName.equals(MonitorConstants.PMC_THREAD_LOG) && fName.startsWith(MonitorConstants.PMC_THREAD_LOG)){
						if(fName.compareTo(threadLog) < 0){
							logger.info("deleting " + fName);
							f.delete();
						}
					}
					else if(!fName.equals(MonitorConstants.PMC_STAGE_LOG) && fName.startsWith(MonitorConstants.PMC_STAGE_LOG)){
						if(fName.compareTo(stageLog) < 0){
							logger.info("deleting " + fName);
							f.delete();
						}
					}
					else if(!fName.equals(MonitorConstants.PMC_SQL_LOG) && fName.startsWith(MonitorConstants.PMC_SQL_LOG)){
						if(fName.compareTo(sqlLog) < 0){
							logger.info("deleting " + fName);
							f.delete();
						}
					}
					else if(!fName.equals(MonitorConstants.PMC_METHOD_LOG) && fName.startsWith(MonitorConstants.PMC_METHOD_LOG)){
						if(fName.compareTo(methodLog) < 0){
							logger.info("deleting " + fName);
							f.delete();
						}
					}
				}
				catch(Exception e){
					logger.error(e.getMessage(), e);
				}
			}
		}
	}
}
