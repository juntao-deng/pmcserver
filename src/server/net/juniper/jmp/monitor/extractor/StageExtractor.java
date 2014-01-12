package net.juniper.jmp.monitor.extractor;

import net.juniper.jmp.tracer.dumper.info.StageInfoBaseDump;
public class StageExtractor extends AbstractInfoExtractor<StageInfoBaseDump>{
	public StageExtractor(String baseDir, String[] sufs, boolean delete) {
		super(baseDir, sufs, delete);
	}
	@Override
	protected StageInfoBaseDump processString(String p) {
		return StageInfoBaseDump.fromString(p);
	}
	@Override
	protected String getFileName() {
		return "pmc_stage.log";
	}

}
