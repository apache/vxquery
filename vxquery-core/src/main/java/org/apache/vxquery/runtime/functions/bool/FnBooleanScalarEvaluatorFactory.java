package org.apache.vxquery.runtime.functions.bool;

import org.apache.vxquery.datamodel.accessors.SequencePointable;
import org.apache.vxquery.datamodel.accessors.TaggedValuePointable;
import org.apache.vxquery.datamodel.values.ValueTag;
import org.apache.vxquery.exceptions.ErrorCode;
import org.apache.vxquery.exceptions.SystemException;
import org.apache.vxquery.runtime.functions.base.AbstractTaggedValueArgumentScalarEvaluator;
import org.apache.vxquery.runtime.functions.base.AbstractTaggedValueArgumentScalarEvaluatorFactory;

import edu.uci.ics.hyracks.algebricks.common.exceptions.AlgebricksException;
import edu.uci.ics.hyracks.algebricks.runtime.base.IScalarEvaluator;
import edu.uci.ics.hyracks.algebricks.runtime.base.IScalarEvaluatorFactory;
import edu.uci.ics.hyracks.api.context.IHyracksTaskContext;
import edu.uci.ics.hyracks.data.std.api.IPointable;
import edu.uci.ics.hyracks.data.std.primitive.IntegerPointable;
import edu.uci.ics.hyracks.data.std.primitive.LongPointable;
import edu.uci.ics.hyracks.data.std.primitive.UTF8StringPointable;

public class FnBooleanScalarEvaluatorFactory extends AbstractTaggedValueArgumentScalarEvaluatorFactory {
    private static final long serialVersionUID = 1L;

    public FnBooleanScalarEvaluatorFactory(IScalarEvaluatorFactory[] args) {
        super(args);
    }

    @Override
    protected IScalarEvaluator createEvaluator(IHyracksTaskContext ctx, IScalarEvaluator[] args)
            throws AlgebricksException {
        final SequencePointable seqp = new SequencePointable();
        final LongPointable lp = (LongPointable) LongPointable.FACTORY.createPointable();
        final IntegerPointable ip = (IntegerPointable) IntegerPointable.FACTORY.createPointable();
        final UTF8StringPointable utf8p = (UTF8StringPointable) UTF8StringPointable.FACTORY.createPointable();
        return new AbstractTaggedValueArgumentScalarEvaluator(args) {
            @Override
            protected void evaluate(TaggedValuePointable[] args, IPointable result) throws SystemException {
                TaggedValuePointable tvp = args[0];
                switch (tvp.getTag()) {
                    case ValueTag.SEQUENCE_TAG: {
                        tvp.getValue(seqp);
                        if (seqp.getEntryCount() == 0) {
                            FnFalseScalarEvaluatorFactory.setFalse(result);
                            return;
                        }
                        FnTrueScalarEvaluatorFactory.setTrue(result);
                        return;
                    }

                    case ValueTag.XS_BOOLEAN_TAG: {
                        result.set(tvp);
                        return;
                    }

                    case ValueTag.XS_LONG_TAG:
                    case ValueTag.XS_INTEGER_TAG: {
                        tvp.getValue(lp);
                        if (lp.longValue() == 0) {
                            FnFalseScalarEvaluatorFactory.setFalse(result);
                            return;
                        }
                        FnTrueScalarEvaluatorFactory.setTrue(result);
                        return;
                    }

                    case ValueTag.XS_INT_TAG: {
                        tvp.getValue(ip);
                        if (ip.intValue() == 0) {
                            FnFalseScalarEvaluatorFactory.setFalse(result);
                            return;
                        }
                        FnTrueScalarEvaluatorFactory.setTrue(result);
                        return;
                    }

                    case ValueTag.XS_STRING_TAG: {
                        tvp.getValue(utf8p);
                        if (utf8p.getUTFLength() == 0) {
                            FnFalseScalarEvaluatorFactory.setFalse(result);
                            return;
                        }
                        FnTrueScalarEvaluatorFactory.setTrue(result);
                        return;
                    }
                }
                throw new SystemException(ErrorCode.FORG0006);
            }
        };
    }
}