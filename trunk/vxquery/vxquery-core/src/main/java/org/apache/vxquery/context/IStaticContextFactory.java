package org.apache.vxquery.context;

import java.io.Serializable;

public interface IStaticContextFactory extends Serializable {
    public StaticContext createStaticContext();
}