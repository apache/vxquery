package org.apache.vxquery.runtime.functions.type;

import org.apache.vxquery.datamodel.accessors.TaggedValuePointable;
import org.apache.vxquery.exceptions.SystemException;
import org.apache.vxquery.runtime.functions.base.AbstractTaggedValueArgumentScalarEvaluator;
import org.apache.vxquery.runtime.functions.base.AbstractTaggedValueArgumentScalarEvaluatorFactory;

import edu.uci.ics.hyracks.algebricks.runtime.base.IScalarEvaluator;
import edu.uci.ics.hyracks.algebricks.runtime.base.IScalarEvaluatorFactory;
import edu.uci.ics.hyracks.data.std.api.IPointable;

public abstract class AbstractTypeScalarEvaluatorFactory extends AbstractTaggedValueArgumentScalarEvaluatorFactory {
    private static final long serialVersionUID = 1L;

    public AbstractTypeScalarEvaluatorFactory(IScalarEvaluatorFactory[] args) {
        super(args);
    }

    protected static abstract class AbstractTypeScalarEvaluator extends AbstractTaggedValueArgumentScalarEvaluator {
        protected AbstractTypeScalarEvaluator(IScalarEvaluator[] args) {
            super(args);
        }

        protected abstract void evaluate(TaggedValuePointable tvp, IPointable result);

        @Override
        protected final void evaluate(TaggedValuePointable[] args, IPointable result) throws SystemException {
            evaluate(args[0], result);
        }
    }
}