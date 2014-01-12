package net.juniper.jmp.monitor.extractor;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

import net.juniper.jmp.tracer.dumper.info.MethodInfoDump;
import net.juniper.jmp.tracer.dumper.info.SqlInfoDump;
import net.juniper.jmp.tracer.dumper.info.StageInfoBaseDump;
import net.juniper.jmp.tracer.dumper.info.ThreadInfoDump;

public class ExtractorHelper {
	public static void mergeAll(List<ThreadInfoDump> threads, List<StageInfoBaseDump> stages, List<SqlInfoDump> sqls, List<MethodInfoDump> methods) {
		if(threads == null || threads.size() == 0)
			return;
		Map<StageInfoBaseDump, StringBuffer> mergedSqlMap = new HashMap<StageInfoBaseDump, StringBuffer>();
		Map<String, ThreadInfoDump> threadsMap = hashliseThreads(threads);
		Map<String, StageInfoBaseDump> stagesMap = hashliseStages(stages);
		if(sqls != null){
			Iterator<SqlInfoDump> sqlIt = sqls.iterator();
			while(sqlIt.hasNext()){
				SqlInfoDump sql = sqlIt.next();
				ThreadInfoDump td = threadsMap.get(sql.getCallId());
				if(td != null){
					doMergeSql(td, mergedSqlMap, sql);
					sql.setStageMethod(td.getStageMethod());
					sql.setStageName(td.getStageName());
					sql.setStagePath(td.getStagePath());
				}
				else{
					StageInfoBaseDump stage = stagesMap.get(sql.getCallId());
					if(stage != null){
						doMergeSql(stage, mergedSqlMap, sql);
						sql.setStageMethod(stage.getStageMethod());
						sql.setStageName(stage.getStageName());
						sql.setStagePath(stage.getStagePath());
					}
				}
			}
		}
		
		threads.get(0).setUserObject(sqls);
		
		if(methods != null){
			Iterator<MethodInfoDump> mit = methods.iterator();
			while(mit.hasNext()){
				MethodInfoDump m = mit.next();
				ThreadInfoDump td = threadsMap.get(m.getCallId());
				if(td != null){
					td.setDetachedMethod(m.getMethod());
				}
				else{
					StageInfoBaseDump stage = stagesMap.get(m.getCallId());
					if(stage != null){
						stage.setDetachedMethod(m.getMethod());
					}
				}
			}
		}
		
		if(stages != null){
			Iterator<StageInfoBaseDump> mit = stages.iterator();
			while(mit.hasNext()){
				StageInfoBaseDump m = mit.next();
				ThreadInfoDump td = threadsMap.get(m.getParentId());
				if(td != null){
					List<StageInfoBaseDump> clist = td.getChildrenStages();
					if(clist == null){
						clist = new ArrayList<StageInfoBaseDump>();
						td.setChildrenStages(clist);
					}
					clist.add(m);
				}
				else{
					StageInfoBaseDump stage = stagesMap.get(m.getParentId());
					if(stage != null){
						List<StageInfoBaseDump> clist = stage.getChildrenStages();
						if(clist == null){
							clist = new ArrayList<StageInfoBaseDump>();
							stage.setChildrenStages(clist);
						}
						clist.add(m);
					}
				}
			}
		}
		
		Iterator<Entry<StageInfoBaseDump, StringBuffer>> entryIt = mergedSqlMap.entrySet().iterator();
		while(entryIt.hasNext()){
			Entry<StageInfoBaseDump, StringBuffer> entry = entryIt.next();
			entry.getKey().setDetachedSql(entry.getValue().toString());
		}
	}

	private static void doMergeSql(StageInfoBaseDump stage, Map<StageInfoBaseDump, StringBuffer> mergedSqlMap, SqlInfoDump sql) {
		StringBuffer sqlBuf = mergedSqlMap.get(stage);
		if(sqlBuf == null){
			sqlBuf = new StringBuffer();
			mergedSqlMap.put(stage, sqlBuf);
		}
		if(sqlBuf.length() != 0)
			sqlBuf.append("\n");
		sqlBuf.append("dbconn:" + sql.getConnId() + ",sql:" + sql.getSql() + ",duration:" + sql.getDuration() + ",resultCount:" + sql.getResultCount());
	}

	private static Map<String, StageInfoBaseDump> hashliseStages(List<StageInfoBaseDump> stages) {
		Map<String, StageInfoBaseDump> map = new HashMap<String, StageInfoBaseDump>();
		Iterator<StageInfoBaseDump> tit = stages.iterator();
		while(tit.hasNext()){
			StageInfoBaseDump stage = tit.next();
			map.put(stage.getCallId(), stage);
		}
		return map;
	}

	private static Map<String, ThreadInfoDump> hashliseThreads(List<ThreadInfoDump> threads) {
		Map<String, ThreadInfoDump> map = new HashMap<String, ThreadInfoDump>();
		Iterator<ThreadInfoDump> tit = threads.iterator();
		while(tit.hasNext()){
			ThreadInfoDump thread = tit.next();
			map.put(thread.getCallId(), thread);
		}
		return map;
	}
}
