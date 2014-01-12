package net.juniper.jmp.monitor.persist;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.Iterator;
import java.util.List;

import net.juniper.jmp.monitor.core.IQueryConstant;
import net.juniper.jmp.tracer.dumper.info.SqlInfoDump;
import net.juniper.jmp.tracer.dumper.info.StageInfoBaseDump;
import net.juniper.jmp.tracer.dumper.info.ThreadInfoDump;

import org.apache.commons.lang.StringUtils;
import org.apache.derby.jdbc.EmbeddedDriver;
import org.apache.log4j.Logger;
/**
 * History log persistence service
 * @author juntaod
 *
 */
public class LogPersister {
	private Logger logger = Logger.getLogger(LogPersister.class);
	private static LogPersister instance = new LogPersister();
	private SimpleDateFormat oriSdf = new SimpleDateFormat("MM/dd/yyyy HH:mm:ss");
	private SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
	static{
		try {
			Class.forName(EmbeddedDriver.class.getName());
		} 
		catch (ClassNotFoundException e) {
			e.printStackTrace();
		}
	}
	
	private LogPersister() {
		
	}
	
	public String getLastUpdateTime() {
		Connection conn = null;
		Statement st = null;
		try{
			conn = getConnection();
			st = conn.createStatement();
			ResultSet rs = st.executeQuery("select version from m_currentversion where id='1'");
			if(rs.next()){
				String result = rs.getString(1);
				return result;
			}
		}
		catch(Exception e){
			logger.error(e.getMessage(), e);
		}
		finally{
			if(st != null){
				try {
					st.close();
				} catch (SQLException e) {
					logger.error(e.getMessage(), e);
				}
			}
			if(conn != null){
				try {
					conn.close();
				} 
				catch (SQLException e) {
					logger.error(e.getMessage(), e);
				}
			}
		}
		throw new RuntimeException("failed to get version from db");
	}
	
	public static LogPersister getInstance() {
		return instance;
	}
	
	private Connection getConnection(){
		try{
			Connection conn = DriverManager.getConnection("jdbc:derby:/var/tmp/monitor/serverdb;create=true");
			return conn;
		}
		catch(Exception e){
			logger.error(e.getMessage(), e);
			throw new RuntimeException("error creating db connection");
		}
	}
	
	public void tryCreateTables(){
		Connection conn = null;
		Statement st = null;
		try{
			conn = getConnection();
			st = conn.createStatement();
			try{
				st.execute("create table m_threadinfo (callid varchar(100) primary key, clientip varchar(100), threadid int, requestbytes int, responsebytes int, sumsqlcount int, sumstagecount int,"
						+ "startts timestamp not null, endts timestamp not null, duration int not null, stagename varchar(500), stagepath varchar(500), stagemethod varchar(500), detachedsql clob, sqls int, conns int not null, stages int, userid varchar(100), methodstack clob, asyncid varchar(200), asynccallid varchar(200), attachid varchar(200), async smallint)");
			}
			catch(Exception e1){
				//logger.error(e1.getMessage(), e1);
			}
			try{
				st.execute("CREATE INDEX m_t_async_startts_endts on m_threadinfo (async, startts, endts desc)");
				st.execute("CREATE INDEX m_t_attachid on m_threadinfo (attachid)");
				st.execute("CREATE INDEX m_t_startts_asc on m_threadinfo (startts)");
				st.execute("CREATE INDEX m_t_duration_startts_endts on m_threadinfo (duration desc, startts, endts desc)");
				st.execute("CREATE INDEX m_t_conns_startts_endts on m_threadinfo (conns desc, startts, endts desc)");
				st.execute("CREATE INDEX m_t_sumsqlcount_startts_endts on m_threadinfo (sumsqlcount desc, startts, endts desc)");
				st.execute("CREATE INDEX m_t_sumstagecount_startts_endts on m_threadinfo (sumstagecount desc, startts, endts desc)");
				st.execute("CREATE INDEX m_t_sumsqlcount ON m_threadinfo (sumsqlcount desc)");
				st.execute("CREATE INDEX m_t_sumstagecount ON m_threadinfo (sumstagecount desc)");
				st.execute("CREATE INDEX m_t_duration ON m_threadinfo (duration desc)");
			}
			catch(Exception e1){
				logger.error(e1.getMessage(), e1);
			}
			try{
				st.execute("create table m_currentversion (id varchar(100) primary key, version varchar(100), updatets varchar(100))");
				st.execute("insert into m_currentversion values ('1', null, null)");
			}
			catch(Exception e1){
				//logger.error(e1.getMessage(), e1);
			}
			try{
				st.execute("create table m_stage (callid varchar(100) primary key, threadcallid varchar(100), pcallid varchar(100), sumsqlcount int, sumstagecount int,"
						+ "startts timestamp, endts timestamp, duration int, stagename varchar(500), stagepath varchar(500), stagemethod varchar(500), detachedsql clob, sqls int, conns int, stages int, methodstack clob, async smallint)");
			}
			catch(Exception e1){
				//logger.error(e1.getMessage(), e1);
			}
			
			try{
				st.execute("CREATE INDEX m_stage_pcallid ON m_stage (pcallid)");
				st.execute("CREATE INDEX m_stage_threadcallid ON m_stage (threadcallid)");
			}
			catch(Exception e1){
				logger.error(e1.getMessage(), e1);
			}
			
			try{
				st.execute("create table m_sql (id int not null PRIMARY KEY GENERATED ALWAYS AS IDENTITY (START WITH 1, INCREMENT BY 1), callid varchar(100), connid varchar(100), sqlstr clob, duration int, resultcount int, stagepath varchar(500), stagemethod varchar(500), stagename varchar(500), startts timestamp, endts timestamp)");
			}
			catch(Exception e1){
//				logger.error(e1.getMessage(), e1);
			}
			
			try{
				st.execute("CREATE INDEX m_sql_duration ON m_sql (duration desc)");
				st.execute("CREATE INDEX m_sql_resultcount ON m_sql (resultcount desc)");
				st.execute("CREATE INDEX m_sql_startts_asc ON m_sql (startts asc)");
			}
			catch(Exception e1){
				logger.error(e1.getMessage(), e1);
			}
		}
		catch(Exception e){
			logger.error(e.getMessage(), e);
		}
		finally{
			if(st != null){
				try {
					st.close();
				} 
				catch (SQLException e) {
					logger.error(e.getMessage(), e);
				}
			}
			if(conn != null){
				try {
					conn.close();
				} 
				catch (SQLException e) {
					logger.error(e.getMessage(), e);
				}
			}
		}
	}
	
	public void persistLog(ThreadInfoDump[] threads, String version){
		if(version == null || version.equals(""))
			return;
		
		Connection conn = null;
		PreparedStatement threadPs = null;
		PreparedStatement stagePs = null;
		PreparedStatement sqlPs = null;
		Statement versionSt = null;
		try {
			conn = getConnection();
			conn.setAutoCommit(false);
			versionSt = conn.createStatement();
			versionSt.executeUpdate("update m_currentversion set version='" + version + "'");
			
			if(threads == null || threads.length == 0)
				return;
			
			logger.info("-------------- persisting logs,version:" + version + ",count:" + threads.length);
			threadPs = conn.prepareStatement("insert into m_threadinfo(callid, clientip, threadid, requestbytes, responsebytes, "
					+ "sumsqlcount, sumstagecount, startts, endts, duration, "
					+ "stagename, stagepath, stagemethod, detachedsql, sqls,"
					+ "conns, stages,userid, methodstack, asyncid, asynccallid, attachid, async) values(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)");
			
			stagePs = conn.prepareStatement("insert into m_stage(callid, threadcallid, pcallid,"
					+ "sumsqlcount, sumstagecount, startts, endts, duration, "
					+ "stagename, stagepath, stagemethod, detachedsql, sqls,"
					+ "conns, stages, methodstack, async) values(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)");
			sqlPs = conn.prepareStatement("insert into m_sql(callid, connid, sqlstr, duration, resultcount, stagepath, stagemethod, stagename, startts, endts) values(?,?,?,?,?,?,?,?,?,?)");
			
			for(int i = 0; i < threads.length; i ++){
				ThreadInfoDump t = threads[i];
				persistThread(threadPs, stagePs, sqlPs, t);
			}
			if(threads[0].getUserObject() != null){
				List<SqlInfoDump> sqls = (List<SqlInfoDump>) threads[0].getUserObject();
				int size = sqls.size();
				for(int i = 0; i < size; i ++){
					SqlInfoDump sql = sqls.get(i);
					persistSql(sqlPs, sql);
				}
			}

			conn.commit();
		} 
		catch (SQLException e) {
			try {
				conn.rollback();
			} 
			catch (SQLException e1) {
				logger.error(e.getMessage(), e);
			}
			logger.error(e.getMessage(), e);
		}
		finally{
			if(versionSt != null){
				try {
					versionSt.close();
				} 
				catch (SQLException e) {
					logger.error(e.getMessage(), e);
				}
			}
			if(threadPs != null){
				try {
					threadPs.close();
				}
				catch (SQLException e) {
					logger.error(e.getMessage(), e);
				}
			}
			if(stagePs != null){
				try {
					stagePs.close();
				} 
				catch (SQLException e) {
					logger.error(e.getMessage(), e);
				}
			}
			if(sqlPs != null){
				try {
					sqlPs.close();
				} catch (SQLException e) {
					logger.error(e.getMessage(), e);
				}
			}
			if(conn != null){
				try {
					conn.close();
				} 
				catch (SQLException e) {
					logger.error(e.getMessage(), e);
				}
			}
		}
	}
	private void persistThread(PreparedStatement threadPs, PreparedStatement stagePs, PreparedStatement sqlPs, ThreadInfoDump t){
		try{
			threadPs.setString(1, t.getCallId());
			threadPs.setString(2, t.getClientIp());
			threadPs.setLong(3, t.getThreadId());
			threadPs.setInt(4, t.getRequestBytes());
			threadPs.setInt(5, t.getResponseBytes());
			
			threadPs.setInt(6, t.getSumSqlCount());
			threadPs.setInt(7, t.getSumStageCount());
			threadPs.setString(8, transTs(t.getStartTs()));
			threadPs.setString(9, transTs(t.getEndTs()));
			threadPs.setLong(10, t.getDuration());
			
			threadPs.setString(11, t.getStageName());
			threadPs.setString(12, t.getStagePath());
			threadPs.setString(13, t.getStageMethod());
			threadPs.setString(14, t.getDetachedSql());
			threadPs.setInt(15, t.getSqls());
			
			threadPs.setInt(16, t.getConns());
			threadPs.setInt(17, t.getStages());
			threadPs.setString(18, t.getUserId());
			threadPs.setString(19, t.getDetachedMethod());
			
			threadPs.setString(20, t.getAsyncId());
			threadPs.setString(21, t.getAsyncCallId());
			threadPs.setString(22, t.getAttachToAsyncId());
			threadPs.setInt(23, t.isAsync() ? 1 : 0);
			threadPs.execute();
			
			if(t.getChildrenStages() != null){
				List<StageInfoBaseDump> stages = t.getChildrenStages();
				int size = stages.size();
				for(int i = 0; i < size; i ++){
					StageInfoBaseDump stage = stages.get(i);
					persistStage(t.getCallId(), stagePs, sqlPs, stage);
				}
			}
		}
		catch(Exception e){
			logger.error("error while persisting thread,id:" + t.getCallId(), e);
		}
	}
	
	private String transTs(String ts) {
		if(ts == null || ts.equals(""))
			return ts;
		try {
			if(ts.length() == 16)
				ts += ":00";
			Date d = oriSdf.parse(ts);
			return sdf.format(d);
		} 
		catch (ParseException e) {
			logger.error(e.getMessage(), e);
		}
		return ts;
	}
	
	private String retransTs(String ts) {
		if(ts == null || ts.equals(""))
			return ts;
		Date d;
		try {
			d = sdf.parse(ts);
			return oriSdf.format(d);
		} 
		catch (ParseException e) {
			logger.error(e.getMessage(), e);
		}
		return ts;
	}

	private void persistSql(PreparedStatement sqlPs, SqlInfoDump sql) {
		try{
			sqlPs.setString(1, sql.getCallId());
			sqlPs.setString(2, sql.getConnId());
			sqlPs.setString(3, sql.getSql());
			sqlPs.setLong(4, sql.getDuration());
			sqlPs.setInt(5, sql.getResultCount());
			sqlPs.setString(6, sql.getStagePath());
			sqlPs.setString(7, sql.getStageMethod());
			sqlPs.setString(8, sql.getStageName());
			sqlPs.setString(9, transTs(sql.getStartTs()));
			sqlPs.setString(10, transTs(sql.getEndTs()));
			sqlPs.execute();
		}
		catch(Exception e){
			logger.error(e.getMessage(), e);
		}
	}
	
	private void persistStage(String threadCallId, PreparedStatement stagePs, PreparedStatement sqlPs, StageInfoBaseDump currStage) {
		try{
			String stageId = currStage.getCallId();
			String parentId = stageId.substring(0, stageId.lastIndexOf("_"));
			stagePs.setString(1, currStage.getCallId());
			stagePs.setString(2, threadCallId);
			stagePs.setString(3, parentId);
			
			
			stagePs.setInt(4, currStage.getSumSqlCount());
			stagePs.setInt(5, currStage.getSumStageCount());
			stagePs.setString(6, transTs(currStage.getStartTs()));
			stagePs.setString(7, transTs(currStage.getEndTs()));
			stagePs.setLong(8, currStage.getDuration());
			
			stagePs.setString(9, currStage.getStageName());
			stagePs.setString(10, currStage.getStagePath());
			stagePs.setString(11, currStage.getStageMethod());
			stagePs.setString(12, currStage.getDetachedSql());
			stagePs.setInt(13, currStage.getSqls());
			
			stagePs.setInt(14, currStage.getConns());
			stagePs.setInt(15, currStage.getStages());
			stagePs.setString(16, currStage.getDetachedMethod());
			stagePs.setInt(17, currStage.isAsync() ? 1 : 0);
			stagePs.execute();
			
			if(currStage.getChildrenStages() != null){
				List<StageInfoBaseDump> stages = currStage.getChildrenStages();
				int size = stages.size();
				for(int i = 0; i < size; i ++){
					StageInfoBaseDump stage = stages.get(i);
					persistStage(threadCallId, stagePs, sqlPs, stage);
				}
			}
			
//			if(currStage.getSqlInfos() != null){
//				List<SqlInfoDump> sqls = currStage.getSqlInfos();
//				int size = sqls.size();
//				for(int i = 0; i < size; i ++){
//					SqlInfoDump sql = sqls.get(i);
//					persistSql(sqlPs, sql);
//				}
//			}
		}
		catch(Exception e){
			logger.error(e.getMessage(), e);
		}
	}


//	private void loadSqls(StageInfoBaseDump parentStage, Connection conn) {
//		String sql = "select * from m_sql t where t.callid = '?'";
//		PreparedStatement st = null;
//		try{
//			List<SqlInfoDump> infoList = new ArrayList<SqlInfoDump>();
//			st = conn.prepareStatement(sql);
//			ResultSet rs = st.executeQuery();
//			while(rs.next()){
//				SqlInfoDump sqlInfo = formSqlInfo(rs);
//				if(sqlInfo != null){
//					infoList.add(sqlInfo);
//				}
//			}
//			parentStage.setSqlInfos(infoList);
//		}
//		catch(Exception e){
//			logger.error(e.getMessage(), e);
//		}
//		finally{
//			if(st != null){
//				try {
//					st.close();
//				} 
//				catch (SQLException e) {
//					logger.error(e.getMessage(), e);
//				}
//			}
//		}
//	}

	public List<StageInfoBaseDump> getStages(String parentId) {
		String sql = "select * from m_stage t where t.pcallid = ?";
		PreparedStatement st = null;
		Connection conn = null;;
		try{
			List<StageInfoBaseDump> stageList = new ArrayList<StageInfoBaseDump>();
			conn = getConnection();
			st = conn.prepareStatement(sql);
			st.setString(1, parentId);
			st.setMaxRows(1000);
			ResultSet rs = st.executeQuery();
			while(rs.next()){
				StageInfoBaseDump stage = formStageInfo(rs);
				if(stage != null){
//					loadStages(stage, conn);
//					loadSqls(stage, conn);
					stageList.add(stage);
				}
			}
//			parentStage.setChildrenStages(infoList);
			return stageList;
		}
		catch(Exception e){
			logger.error(e.getMessage(), e);
		}
		finally{
			if(st != null){
				try {
					st.close();
				} 
				catch (SQLException e) {
					logger.error(e.getMessage(), e);
				}
			}
			if(conn != null){
				try {
					conn.close();
				} catch (SQLException e) {
					logger.error(e.getMessage(), e);
				}
			}
		}
		return null;
	}


	private ThreadInfoDump formThreadInfo(ResultSet rs) throws SQLException {
		String callId = rs.getString("callid");
		long threadId = rs.getLong("threadid");
		String stageName = rs.getString("stagename");
		String startTs = retransTs(rs.getString("startts"));
		String endTs = retransTs(rs.getString("endTs"));
		String clientIp = rs.getString("clientIp");
		int conns = rs.getInt("conns");
		int duration = rs.getInt("duration");
		int requestBytes = rs.getInt("requestbytes");
		int responseBytes = rs.getInt("responsebytes");
		String stageMethod = rs.getString("stagemethod");
		String stagePath = rs.getString("stagepath");
		int sumSqlCount = rs.getInt("sumsqlcount");
		int sumStageCount = rs.getInt("sumstagecount");
		String userId = rs.getString("userid");
		String detachedSql = rs.getString("detachedsql");
		int stages = rs.getInt("stages");
		int sqls = rs.getInt("sqls");
		String method = rs.getString("methodstack");
		String asyncId = rs.getString("asyncid");
		String asyncCallId = rs.getString("asynccallid");
		String attachId = rs.getString("attachid");
		int async = rs.getInt("async");
		
		ThreadInfoDump threadInfo = new ThreadInfoDump(callId, stageName, threadId);
		threadInfo.setStartTs(startTs);
		threadInfo.setEndTs(endTs);
		threadInfo.setClientIp(clientIp);
		threadInfo.setConns(conns);
		threadInfo.setDuration(duration);
		threadInfo.setRequestBytes(requestBytes);
		threadInfo.setResponseBytes(responseBytes);
		threadInfo.setStageMethod(stageMethod);
		threadInfo.setStagePath(stagePath);
		threadInfo.setSumSqlCount(sumSqlCount);
		threadInfo.setSumStageCount(sumStageCount);
		threadInfo.setUserId(userId);
		threadInfo.setStages(stages);
		threadInfo.setSqls(sqls);
		threadInfo.setDetachedSql(detachedSql);
//		MethodInfoDump m = new MethodInfoDump(callId, method);
		threadInfo.setDetachedMethod(method);
		threadInfo.setAttachToAsyncId(attachId);
		threadInfo.setAsyncCallId(asyncCallId);
		threadInfo.setAsyncId(asyncId);
		threadInfo.setAsync(async == 1);
		return threadInfo;
	}
	
	private StageInfoBaseDump formStageInfo(ResultSet rs) throws SQLException{
		String pcallId = rs.getString("pcallid");
		String callId = rs.getString("callid");
		String stageName = rs.getString("stagename");
		String startTs = retransTs(rs.getString("startts"));
		String endTs = retransTs(rs.getString("endTs"));
		int conns = rs.getInt("conns");
		int duration = rs.getInt("duration");
		String stageMethod = rs.getString("stagemethod");
		String stagePath = rs.getString("stagepath");
		int sumSqlCount = rs.getInt("sumsqlcount");
		int sumStageCount = rs.getInt("sumstagecount");
		String detachedSql = rs.getString("detachedsql");
		int stages = rs.getInt("stages");
		int sqls = rs.getInt("sqls");
		String method = rs.getString("methodstack");
		int async = rs.getInt("async");
		StageInfoBaseDump stage = new StageInfoBaseDump(pcallId, callId, stageName);
		stage.setStartTs(startTs);
		stage.setEndTs(endTs);
		stage.setConns(conns);
		stage.setDuration(duration);
		stage.setStageMethod(stageMethod);
		stage.setStagePath(stagePath);
		stage.setSumSqlCount(sumSqlCount);
		stage.setSumStageCount(sumStageCount);
		stage.setStages(stages);
		stage.setSqls(sqls);
		stage.setDetachedSql(detachedSql);
//		MethodInfoDump m = new MethodInfoDump(callId, method);
		stage.setDetachedMethod(method);
		stage.setAsync(async == 1);
		return stage;
	}

	public List<ThreadInfoDump> getThreadInfos(String startTs, String endTs, String whereClause) {
		return doGetThreadInfos(startTs, endTs, whereClause);
	}
	
	private List<ThreadInfoDump> doGetThreadInfos(String startTs, String endTs, String whereClause){
		startTs = transTs(startTs);
		endTs = transTs(endTs);
		String sql = null;
		boolean needFilter = false;
		//must write sql like below to use index, or it will be too slow.
		if(whereClause != null && !whereClause.equals("")){
			if(IQueryConstant.WHERE_DURATION.equals(whereClause)){
				sql = "select * from m_threadinfo t where t.duration <= 10000000 and t.startts >= '" + startTs + "' and t.endts <= '" + endTs + "' order by t.duration desc";
			}
			else if(IQueryConstant.WHERE_SQL.equals(whereClause)){
				sql = "select * from m_threadinfo t where t.sumsqlcount <= 10000000 and t.startts >= '" + startTs + "' and t.endts <= '" + endTs + "' order by t.sumsqlcount desc";
			}
			else if(IQueryConstant.WHERE_DBCONNS.equals(whereClause)){
				sql = "select * from m_threadinfo t where t.conns <= 10000000 and t.startts >= '" + startTs + "' and t.endts <= '" + endTs + "' order by t.conns desc";
			}
			else if(IQueryConstant.WHERE_STAGES.equals(whereClause)){
				sql = "select * from m_threadinfo t where t.sumstagecount <= 10000000 and t.startts >= '" + startTs + "' and t.endts <= '" + endTs + "' order by t.sumstagecount desc";
			}
			else if(IQueryConstant.WHERE_ASYNC.equals(whereClause)){
				sql = "select * from m_threadinfo t where t.async = 1 and t.startts >= '" + startTs + "' and t.endts <= '" + endTs + "' order by t.startts asc";
				needFilter = true;
			}
//			else if(whereClause.startsWith(IQueryConstant.WHERE_USER)){
//				String userId = whereClause.split(":")[1];
//				
//				sql += " and t.userid='" + userId + "' order by t.starts asc";
//			}
		}
		else{
			sql = "select * from m_threadinfo t where t.startts >= '" + startTs + "' and t.endts <= '" + endTs + "' order by t.startts asc";
			needFilter = true;
		}
		Connection conn = null;
		Statement st = null;
		try{
			conn = getConnection();
			logger.info("### got connection for sql:" + sql);
//			long startTime = System.currentTimeMillis();
			List<ThreadInfoDump> infoList = new ArrayList<ThreadInfoDump>();
			st = conn.createStatement();
//			st.setMaxRows(5000);
			ResultSet rs = st.executeQuery(sql);
			int ptCount = 0;
			while(rs.next() && ptCount < 500){
				ThreadInfoDump threadInfo = formThreadInfo(rs);
				if(threadInfo != null){
					if(needFilter){
						if(threadInfo.getAttachToAsyncId() == null){
							ptCount ++;
							infoList.add(threadInfo);
							logger.info("### threadinfo:" + threadInfo.getAsyncId());
						}
					}
					else{
						threadInfo.setAttachToAsyncId(null);
						ptCount ++;
						infoList.add(threadInfo);
					}
				}
			}
			
			if(needFilter){
				if(infoList.size() > 0){
					List<String> inList = new ArrayList<String>();
					Iterator<ThreadInfoDump> tit = infoList.iterator();
					while(tit.hasNext()){
						ThreadInfoDump thread = tit.next();
						String asyncId = thread.getAsyncId();
						if(asyncId != null){
							inList.add("'" + asyncId + "'");
						}
					}
					if(inList.size() > 0){
						sql = "select * from m_threadinfo t where t.attachid in (" + StringUtils.join(inList, ",") + ")";
						logger.info("### got connection for sql:" + sql);
						rs = st.executeQuery(sql);
						while(rs.next()){
							ThreadInfoDump threadInfo = formThreadInfo(rs);
							if(threadInfo != null){
								infoList.add(threadInfo);
							}
						}
					}
				}
			}
			
			logger.info("### got result count:" + infoList.size());
//			List<StageInfoBaseDump> stages = loadStages(infoList, conn);
//			long endTime = System.currentTimeMillis();
//			logger.info("### History thread, thread count:" + infoList.size() + " and stages count:" + stages.size() + ", time:" + (endTime - startTime) + "ms");
//			ExtractorHelper.mergeAll(infoList, stages, null, null);
			return infoList;
		}
		catch(Exception e){
			logger.error(e.getMessage(), e);
		}
		finally{
			if(st != null){
				try {
					st.close();
				} 
				catch (SQLException e) {
					logger.error(e.getMessage(), e);
				}
			}
			if(conn != null){
				try {
					conn.close();
				} catch (SQLException e) {
					logger.error(e.getMessage(), e);
				}
			}
		}
		return null;
	}
	

	public List<SqlInfoDump> getSqlInfos(String startTs, String endTs, String wherePart) {
		startTs = transTs(startTs);
		endTs = transTs(endTs);
		String sql = "select * from m_sql t where t.startts >= '" + startTs + "' and t.endts <= '" + endTs + "'";
		if(wherePart != null){
			if(wherePart.equals("duration"))
				sql += " order by t.duration desc, t.startts asc";
			else if(wherePart.equals("resultcount"))
				sql += " order by t.resultcount desc, t.startts asc";
		}
		else
			sql += " order by t.startts asc";
		Connection conn = null;
		Statement st = null;
		try{
			conn = getConnection();
			logger.info("### got connection for sql:" + sql);
			List<SqlInfoDump> infoList = new ArrayList<SqlInfoDump>();
			st = conn.createStatement();
			st.setMaxRows(500);
			ResultSet rs = st.executeQuery(sql);
			while(rs.next()){
				SqlInfoDump threadInfo = formSqlInfo(rs);
				if(threadInfo != null){
					infoList.add(threadInfo);
				}
			}
			return infoList;
		}
		catch(Exception e){
			logger.error(e.getMessage(), e);
		}
		finally{
			if(st != null){
				try {
					st.close();
				} 
				catch (SQLException e) {
					logger.error(e.getMessage(), e);
				}
			}
			if(conn != null){
				try {
					conn.close();
				} catch (SQLException e) {
					logger.error(e.getMessage(), e);
				}
			}
		}
		return null;
	}
	
	private SqlInfoDump formSqlInfo(ResultSet rs) throws SQLException{
		Integer id = rs.getInt("id");
		String callId = rs.getString("callid");
		String connId = rs.getString("connid");
		String sqlStr = rs.getString("sqlstr");
		String startTs = retransTs(rs.getString("startts"));
		String endTs = retransTs(rs.getString("endTs"));
		int duration = rs.getInt("duration");
		int resultCount = rs.getInt("resultcount");
		String stagePath = rs.getString("stagepath");
		String stageMethod = rs.getString("stagemethod");
		String stageName = rs.getString("stagename");
		
		SqlInfoDump sqlInfo = new SqlInfoDump(callId, connId, sqlStr);
		sqlInfo.setDuration(duration);
		sqlInfo.setResultCount(resultCount);
		sqlInfo.setStartTs(startTs);
		sqlInfo.setEndTs(endTs);
		sqlInfo.setId(id);
		sqlInfo.setStagePath(stagePath);
		sqlInfo.setStageMethod(stageMethod);
		sqlInfo.setStageName(stageName);
		return sqlInfo;
	}

	public StageInfoBaseDump getStage(String callId) {
		String tsql = "select * from m_threadinfo t where t.callid = ?";
		PreparedStatement tst = null;
		PreparedStatement st = null;
		Connection conn = null;;
		try{
			StageInfoBaseDump stage = null;
			conn = getConnection();
			tst = conn.prepareStatement(tsql);
			tst.setString(1, callId);
			ResultSet rs = tst.executeQuery();
			if(rs.next()){
				stage = formThreadInfo(rs);
			}
			else{
				String sql = "select * from m_stage t where t.callid = ?";
				st = conn.prepareStatement(sql);
				st.setString(1, callId);
				rs = st.executeQuery();
				if(rs.next()){
					stage = formStageInfo(rs);
				}
			}
//			parentStage.setChildrenStages(infoList);
			return stage;
		}
		catch(Exception e){
			logger.error(e.getMessage(), e);
		}
		finally{
			if(st != null){
				try {
					st.close();
				} 
				catch (SQLException e) {
					logger.error(e.getMessage(), e);
				}
			}
			if(conn != null){
				try {
					conn.close();
				} catch (SQLException e) {
					logger.error(e.getMessage(), e);
				}
			}
		}
		return null;
	}
	
	public static void main(String[] args){
		System.setProperty("derby.language.logQueryPlan", "true");
		LogPersister.getInstance().tryCreateTables();
		LogPersister.getInstance().getThreadInfos("12/25/2013 00:00:00", "12/12/2014 00:00:00", "async");
	}
}
