package org.apache.vxquery.context;

class ThinStaticContextImplFactory implements IStaticContextFactory {
    private static final long serialVersionUID = 1L;

    private final IStaticContextFactory delegateSCFactory;

    private ThinStaticContextImplFactory(IStaticContextFactory delegateSCFactory) {
        this.delegateSCFactory = delegateSCFactory;
    }

    @Override
    public StaticContext createStaticContext() {
        return new ThinStaticContextImpl(delegateSCFactory.createStaticContext());
    }

    static IStaticContextFactory createInstance(ThinStaticContextImpl staticContextImpl) {
        IStaticContextFactory delegateSCFactory = staticContextImpl.getParent().createFactory();
        return new ThinStaticContextImplFactory(delegateSCFactory);
    }
}