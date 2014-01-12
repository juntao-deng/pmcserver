package net.juniper.jmp.monitor.extractor;

import net.juniper.jmp.tracer.dumper.info.MethodInfoDump;

/**
 * 
 * @author juntaod
 *
 */
public class MethodInfoExtractor extends AbstractInfoExtractor<MethodInfoDump> {

	public MethodInfoExtractor(String baseDir, String[] sufs, boolean delete) {
		super(baseDir, sufs, delete);
	}

	@Override
	protected MethodInfoDump processString(String p) {
		return MethodInfoDump.fromString(p);
	}

	@Override
	protected String getFileName() {
		return "pmc_method.log";
	}

}
