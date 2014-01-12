package net.juniper.jmp.monitor.service.impl;

import java.util.List;

import net.juniper.jmp.monitor.persist.LogPersister;
import net.juniper.jmp.monitor.service.ILogService;
import net.juniper.jmp.tracer.dumper.info.SqlInfoDump;
import net.juniper.jmp.tracer.dumper.info.StageInfoBaseDump;
import net.juniper.jmp.tracer.dumper.info.ThreadInfoDump;
/**
 * 
 * @author juntaod
 *
 */
public class LogServiceImpl implements ILogService {

	@Override
	public ThreadInfoDump[] getThreadInfosByPeriod(String startTs, String endTs, String wherePart) {
		List<ThreadInfoDump> resultList = LogPersister.getInstance().getThreadInfos(startTs, endTs, wherePart);
		return resultList == null ? null : resultList.toArray(new ThreadInfoDump[0]);
	}

	@Override
	public StageInfoBaseDump[] getStagesByParentId(String pid) {
		List<StageInfoBaseDump> resultList = LogPersister.getInstance().getStages(pid);
		return resultList == null ? null : resultList.toArray(new StageInfoBaseDump[0]);
	}

	@Override
	public SqlInfoDump[] getSqlInfos(String startTs, String endTs, String wherePart) {
		List<SqlInfoDump> resultList = LogPersister.getInstance().getSqlInfos(startTs, endTs, wherePart);
		return resultList == null ? null : resultList.toArray(new SqlInfoDump[0]);
	}

	@Override
	public StageInfoBaseDump getStageById(String callId) {
		return LogPersister.getInstance().getStage(callId);
	}

}
