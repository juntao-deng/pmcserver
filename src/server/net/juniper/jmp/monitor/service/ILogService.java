package net.juniper.jmp.monitor.service;

import net.juniper.jmp.tracer.dumper.info.SqlInfoDump;
import net.juniper.jmp.tracer.dumper.info.StageInfoBaseDump;
import net.juniper.jmp.tracer.dumper.info.ThreadInfoDump;
/**
 * 
 * @author juntaod
 *
 */
public interface ILogService {
//	public ThreadInfoDump[] getThreadInfosByPeriod(String startTs, String endTs);
	public ThreadInfoDump[] getThreadInfosByPeriod(String startTs, String endTs, String userId);
	public StageInfoBaseDump[] getStagesByParentId(String pid);
	public SqlInfoDump[] getSqlInfos(String startTs, String endTs, String wherePart);
	public StageInfoBaseDump getStageById(String callId);
}
