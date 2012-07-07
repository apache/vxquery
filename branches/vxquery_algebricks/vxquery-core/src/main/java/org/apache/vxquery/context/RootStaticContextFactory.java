package org.apache.vxquery.context;

public class RootStaticContextFactory implements IStaticContextFactory {
    private static final long serialVersionUID = 1L;

    public static final IStaticContextFactory INSTANCE = new RootStaticContextFactory();

    private RootStaticContextFactory() {
    }

    @Override
    public StaticContext createStaticContext() {
        return RootStaticContextImpl.INSTANCE;
    }
}