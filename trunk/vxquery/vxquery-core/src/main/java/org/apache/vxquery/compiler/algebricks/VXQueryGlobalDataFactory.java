package org.apache.vxquery.compiler.algebricks;

import org.apache.vxquery.context.IDynamicContextFactory;

import edu.uci.ics.hyracks.api.context.IHyracksJobletContext;
import edu.uci.ics.hyracks.api.job.IGlobalJobDataFactory;

public class VXQueryGlobalDataFactory implements IGlobalJobDataFactory {
    private static final long serialVersionUID = 1L;

    private final IDynamicContextFactory dcf;

    public VXQueryGlobalDataFactory(IDynamicContextFactory dcf) {
        this.dcf = dcf;
    }

    @Override
    public Object createGlobalJobData(IHyracksJobletContext ctx) {
        return dcf.createDynamicContext(ctx);
    }
}