package org.apache.vxquery.datamodel.builders.jsonItem;

import java.io.IOException;

import org.apache.hyracks.data.std.api.IMutableValueStorage;

public abstract class JsonAbstractBuilder {
    public abstract void reset(IMutableValueStorage mvs);

    public abstract void finish() throws IOException;

    public abstract int getValueTag();
}
