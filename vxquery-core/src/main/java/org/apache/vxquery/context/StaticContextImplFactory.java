package org.apache.vxquery.context;

import java.util.ArrayList;
import java.util.List;

import org.apache.vxquery.types.SequenceType;

class StaticContextImplFactory implements IStaticContextFactory {
    private static final long serialVersionUID = 1L;

    private final IStaticContextFactory parentSCFactory;

    private final List<SequenceType> seqTypes;

    private StaticContextImplFactory(IStaticContextFactory parentSCFactory, List<SequenceType> seqTypes) {
        this.parentSCFactory = parentSCFactory;
        this.seqTypes = seqTypes;
    }

    @Override
    public StaticContext createStaticContext() {
        StaticContextImpl sctx = new StaticContextImpl(parentSCFactory.createStaticContext());
        for (SequenceType sType : seqTypes) {
            sctx.encodeSequenceType(sType);
        }
        return sctx;
    }

    static IStaticContextFactory createInstance(StaticContextImpl staticContextImpl) {
        IStaticContextFactory parentSCFactory = staticContextImpl.getParent().createFactory();
        return new StaticContextImplFactory(parentSCFactory, new ArrayList<SequenceType>(
                staticContextImpl.getSequenceTypeList()));
    }
}