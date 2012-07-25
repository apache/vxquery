package org.apache.vxquery.runtime.functions.base;

import edu.uci.ics.hyracks.algebricks.common.exceptions.AlgebricksException;
import edu.uci.ics.hyracks.algebricks.runtime.base.IScalarEvaluator;
import edu.uci.ics.hyracks.algebricks.runtime.base.IScalarEvaluatorFactory;
import edu.uci.ics.hyracks.api.context.IHyracksTaskContext;

public abstract class AbstractTaggedValueArgumentScalarEvaluatorFactory implements IScalarEvaluatorFactory {
    private static final long serialVersionUID = 1L;

    private final IScalarEvaluatorFactory[] args;

    public AbstractTaggedValueArgumentScalarEvaluatorFactory(IScalarEvaluatorFactory[] args) {
        this.args = args;
    }

    @Override
    public final IScalarEvaluator createScalarEvaluator(IHyracksTaskContext ctx) throws AlgebricksException {
        IScalarEvaluator[] es = new IScalarEvaluator[args.length];
        for (int i = 0; i < es.length; ++i) {
            es[i] = args[i].createScalarEvaluator(ctx);
        }
        return createEvaluator(ctx, es);
    }

    protected abstract IScalarEvaluator createEvaluator(IHyracksTaskContext ctx, IScalarEvaluator[] args)
            throws AlgebricksException;
}