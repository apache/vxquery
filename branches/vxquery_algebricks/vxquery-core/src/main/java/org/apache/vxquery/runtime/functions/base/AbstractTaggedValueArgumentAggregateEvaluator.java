package org.apache.vxquery.runtime.functions.base;

import org.apache.vxquery.datamodel.accessors.TaggedValuePointable;
import org.apache.vxquery.exceptions.SystemException;

import edu.uci.ics.hyracks.algebricks.common.exceptions.AlgebricksException;
import edu.uci.ics.hyracks.algebricks.runtime.base.IAggregateEvaluator;
import edu.uci.ics.hyracks.algebricks.runtime.base.IScalarEvaluator;
import edu.uci.ics.hyracks.dataflow.common.data.accessors.IFrameTupleReference;

public abstract class AbstractTaggedValueArgumentAggregateEvaluator implements IAggregateEvaluator {
    private final IScalarEvaluator[] args;

    private final TaggedValuePointable[] tvps;

    public AbstractTaggedValueArgumentAggregateEvaluator(IScalarEvaluator[] args) {
        this.args = args;
        tvps = new TaggedValuePointable[args.length];
        for (int i = 0; i < tvps.length; ++i) {
            tvps[i] = new TaggedValuePointable();
        }
    }

    @Override
    public void step(IFrameTupleReference tuple) throws AlgebricksException {
        for (int i = 0; i < args.length; ++i) {
            args[i].evaluate(tuple, tvps[i]);
        }
        try {
            step(tvps);
        } catch (SystemException e) {
            throw new AlgebricksException(e);
        }
    }

    protected abstract void step(TaggedValuePointable[] args) throws SystemException;
}