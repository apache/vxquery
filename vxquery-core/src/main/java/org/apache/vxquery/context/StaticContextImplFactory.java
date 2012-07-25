package org.apache.vxquery.context;

class StaticContextImplFactory implements IStaticContextFactory {
    private static final long serialVersionUID = 1L;

    private final IStaticContextFactory parentSCFactory;

    private StaticContextImplFactory(IStaticContextFactory parentSCFactory) {
        this.parentSCFactory = parentSCFactory;
    }

    @Override
    public StaticContext createStaticContext() {
        return new StaticContextImpl(parentSCFactory.createStaticContext());
    }

    static IStaticContextFactory createInstance(StaticContextImpl staticContextImpl) {
        IStaticContextFactory parentSCFactory = staticContextImpl.getParent().createFactory();
        return new StaticContextImplFactory(parentSCFactory);
    }
}