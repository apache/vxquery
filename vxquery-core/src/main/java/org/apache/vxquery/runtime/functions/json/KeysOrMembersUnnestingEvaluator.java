package org.apache.vxquery.runtime.functions.json;

import org.apache.hyracks.algebricks.common.exceptions.AlgebricksException;
import org.apache.hyracks.algebricks.runtime.base.IScalarEvaluator;
import org.apache.hyracks.api.context.IHyracksTaskContext;
import org.apache.hyracks.data.std.api.IPointable;
import org.apache.vxquery.datamodel.accessors.TaggedValuePointable;
import org.apache.vxquery.exceptions.SystemException;
import org.apache.vxquery.runtime.functions.base.AbstractTaggedValueArgumentUnnestingEvaluator;

public class KeysOrMembersUnnestingEvaluator extends AbstractTaggedValueArgumentUnnestingEvaluator {
    private final KeysOrMembersUnnesting keysOrMembersStep;

    public KeysOrMembersUnnestingEvaluator(IHyracksTaskContext ctx, IScalarEvaluator[] args) {
        super(args);
        keysOrMembersStep = new KeysOrMembersUnnesting(ctx, ppool);
    }

    @Override
    public boolean step(IPointable result) throws AlgebricksException {
        return keysOrMembersStep.step(result);
    }

    @Override
    protected void init(TaggedValuePointable[] args) throws SystemException {
        keysOrMembersStep.init(args);

    }
}
