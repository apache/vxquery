package org.apache.vxquery.runtime.functions.base;

import org.apache.vxquery.datamodel.accessors.TaggedValuePointable;

import edu.uci.ics.hyracks.algebricks.common.exceptions.AlgebricksException;
import edu.uci.ics.hyracks.algebricks.runtime.base.IScalarEvaluator;
import edu.uci.ics.hyracks.algebricks.runtime.base.IUnnestingEvaluator;
import edu.uci.ics.hyracks.dataflow.common.data.accessors.IFrameTupleReference;

public abstract class AbstractTaggedValueArgumentUnnestingEvaluator implements IUnnestingEvaluator {
    private final IScalarEvaluator[] args;

    protected final TaggedValuePointable[] tvps;

    public AbstractTaggedValueArgumentUnnestingEvaluator(IScalarEvaluator[] args) {
        this.args = args;
        tvps = new TaggedValuePointable[args.length];
        for (int i = 0; i < tvps.length; ++i) {
            tvps[i] = new TaggedValuePointable();
        }
    }

    @Override
    public final void init(IFrameTupleReference tuple) throws AlgebricksException {
        for (int i = 0; i < args.length; ++i) {
            args[i].evaluate(tuple, tvps[i]);
        }
        init(tvps);
    }

    protected abstract void init(TaggedValuePointable[] args);
}