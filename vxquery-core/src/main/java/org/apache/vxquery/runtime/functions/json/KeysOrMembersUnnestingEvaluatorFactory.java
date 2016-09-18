package org.apache.vxquery.runtime.functions.json;

import org.apache.hyracks.algebricks.common.exceptions.AlgebricksException;
import org.apache.hyracks.algebricks.runtime.base.IScalarEvaluator;
import org.apache.hyracks.algebricks.runtime.base.IScalarEvaluatorFactory;
import org.apache.hyracks.algebricks.runtime.base.IUnnestingEvaluator;
import org.apache.hyracks.api.context.IHyracksTaskContext;
import org.apache.vxquery.runtime.functions.base.AbstractTaggedValueArgumentUnnestingEvaluatorFactory;

public class KeysOrMembersUnnestingEvaluatorFactory extends AbstractTaggedValueArgumentUnnestingEvaluatorFactory {
    private static final long serialVersionUID = 1L;

    public KeysOrMembersUnnestingEvaluatorFactory(IScalarEvaluatorFactory[] args) {
        super(args);
    }

    @Override
    protected IUnnestingEvaluator createEvaluator(IHyracksTaskContext ctx, IScalarEvaluator[] args)
            throws AlgebricksException {
        return new KeysOrMembersUnnestingEvaluator(ctx, args);
    }

}
