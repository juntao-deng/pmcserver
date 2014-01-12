package net.juniper.jmp.monitor.extractor;

import net.juniper.jmp.tracer.dumper.info.SqlInfoDump;
/**
 * 
 * @author juntaod
 *
 */
public class SqlInfoExtractor extends AbstractInfoExtractor<SqlInfoDump> {

	public SqlInfoExtractor(String baseDir, String[] sufs, boolean delete) {
		super(baseDir, sufs, delete);
	}

	@Override
	protected SqlInfoDump processString(String p) {
		return SqlInfoDump.fromString(p);
	}

	@Override
	protected String getFileName() {
		return "pmc_sql.log";
	}

}
