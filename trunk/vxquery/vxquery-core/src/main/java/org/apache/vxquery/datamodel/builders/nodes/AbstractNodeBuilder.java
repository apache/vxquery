package org.apache.vxquery.datamodel.builders.nodes;

import java.io.IOException;

import edu.uci.ics.hyracks.data.std.api.IMutableValueStorage;

public abstract class AbstractNodeBuilder {
    public abstract void reset(IMutableValueStorage mvs) throws IOException;

    public abstract void finish() throws IOException;
}