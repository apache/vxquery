package org.apache.vxquery.runtime.functions.base;

import edu.uci.ics.hyracks.algebricks.common.exceptions.AlgebricksException;
import edu.uci.ics.hyracks.algebricks.runtime.base.IScalarEvaluator;
import edu.uci.ics.hyracks.algebricks.runtime.base.IScalarEvaluatorFactory;
import edu.uci.ics.hyracks.algebricks.runtime.base.IUnnestingEvaluator;
import edu.uci.ics.hyracks.algebricks.runtime.base.IUnnestingEvaluatorFactory;
import edu.uci.ics.hyracks.api.context.IHyracksTaskContext;

public abstract class AbstractTaggedValueArgumentUnnestingEvaluatorFactory implements IUnnestingEvaluatorFactory {
    private static final long serialVersionUID = 1L;

    private final IScalarEvaluatorFactory[] args;

    public AbstractTaggedValueArgumentUnnestingEvaluatorFactory(IScalarEvaluatorFactory[] args) {
        this.args = args;
    }

    @Override
    public final IUnnestingEvaluator createUnnestingEvaluator(IHyracksTaskContext ctx) throws AlgebricksException {
        IScalarEvaluator[] es = new IScalarEvaluator[args.length];
        for (int i = 0; i < es.length; ++i) {
            es[i] = args[i].createScalarEvaluator(ctx);
        }
        return createEvaluator(es);
    }

    protected abstract IUnnestingEvaluator createEvaluator(IScalarEvaluator[] args) throws AlgebricksException;
}