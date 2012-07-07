package org.apache.vxquery.context;

import java.io.Serializable;

import edu.uci.ics.hyracks.api.context.IHyracksJobletContext;

public interface IDynamicContextFactory extends Serializable {
    public DynamicContext createDynamicContext(IHyracksJobletContext ctx);
}