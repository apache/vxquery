package org.apache.vxquery.runtime.functions.type;

import org.apache.vxquery.context.DynamicContext;
import org.apache.vxquery.datamodel.accessors.TaggedValuePointable;
import org.apache.vxquery.datamodel.values.ValueTag;
import org.apache.vxquery.exceptions.SystemException;
import org.apache.vxquery.runtime.functions.base.AbstractTaggedValueArgumentScalarEvaluator;
import org.apache.vxquery.runtime.functions.base.AbstractTaggedValueArgumentScalarEvaluatorFactory;
import org.apache.vxquery.types.SequenceType;

import edu.uci.ics.hyracks.algebricks.runtime.base.IScalarEvaluator;
import edu.uci.ics.hyracks.algebricks.runtime.base.IScalarEvaluatorFactory;
import edu.uci.ics.hyracks.api.context.IHyracksTaskContext;
import edu.uci.ics.hyracks.data.std.api.IPointable;
import edu.uci.ics.hyracks.data.std.primitive.IntegerPointable;

public abstract class AbstractTypeScalarEvaluatorFactory extends AbstractTaggedValueArgumentScalarEvaluatorFactory {
    private static final long serialVersionUID = 1L;

    public AbstractTypeScalarEvaluatorFactory(IScalarEvaluatorFactory[] args) {
        super(args);
    }

    protected static abstract class AbstractTypeScalarEvaluator extends AbstractTaggedValueArgumentScalarEvaluator {
        protected final DynamicContext dCtx;

        private final IntegerPointable ip;

        boolean first;

        protected AbstractTypeScalarEvaluator(IScalarEvaluator[] args, IHyracksTaskContext ctx) {
            super(args);
            dCtx = (DynamicContext) ctx.getJobletContext().getGlobalJobData();
            ip = (IntegerPointable) IntegerPointable.FACTORY.createPointable();
            first = true;
        }

        protected abstract void setSequenceType(SequenceType sType);

        protected abstract void evaluate(TaggedValuePointable tvp, IPointable result);

        @Override
        protected final void evaluate(TaggedValuePointable[] args, IPointable result) throws SystemException {
            if (first) {
                if (args[1].getTag() != ValueTag.XS_INT_TAG) {
                    throw new IllegalArgumentException("Expected int value tag, got: " + args[1].getTag());
                }
                args[1].getValue(ip);
                int typeCode = ip.getInteger();
                SequenceType sType = dCtx.getStaticContext().lookupSequenceType(typeCode);
                setSequenceType(sType);
                first = false;
            }
            evaluate(args[0], result);
        }
    }
}