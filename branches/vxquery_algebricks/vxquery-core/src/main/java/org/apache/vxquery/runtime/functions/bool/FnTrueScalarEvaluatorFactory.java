package org.apache.vxquery.runtime.functions.bool;

import org.apache.vxquery.datamodel.accessors.TaggedValuePointable;
import org.apache.vxquery.datamodel.values.ValueTag;
import org.apache.vxquery.exceptions.SystemException;
import org.apache.vxquery.runtime.functions.base.AbstractTaggedValueArgumentScalarEvaluator;
import org.apache.vxquery.runtime.functions.base.AbstractTaggedValueArgumentScalarEvaluatorFactory;

import edu.uci.ics.hyracks.algebricks.common.exceptions.AlgebricksException;
import edu.uci.ics.hyracks.algebricks.runtime.base.IScalarEvaluator;
import edu.uci.ics.hyracks.algebricks.runtime.base.IScalarEvaluatorFactory;
import edu.uci.ics.hyracks.api.context.IHyracksTaskContext;
import edu.uci.ics.hyracks.data.std.api.IPointable;
import edu.uci.ics.hyracks.data.std.primitive.BooleanPointable;

public class FnTrueScalarEvaluatorFactory extends AbstractTaggedValueArgumentScalarEvaluatorFactory {
    private static final long serialVersionUID = 1L;

    private static final byte[] BOOLEAN_TRUE_CONSTANT;

    static {
        BOOLEAN_TRUE_CONSTANT = new byte[2];
        BOOLEAN_TRUE_CONSTANT[0] = ValueTag.XS_BOOLEAN_TAG;
        BooleanPointable.setBoolean(BOOLEAN_TRUE_CONSTANT, 1, true);
    }

    public static void setTrue(IPointable p) {
        p.set(BOOLEAN_TRUE_CONSTANT, 0, 2);
    }

    public FnTrueScalarEvaluatorFactory(IScalarEvaluatorFactory[] args) {
        super(args);
    }

    @Override
    protected IScalarEvaluator createEvaluator(IHyracksTaskContext ctx, IScalarEvaluator[] args)
            throws AlgebricksException {
        return new AbstractTaggedValueArgumentScalarEvaluator(args) {
            @Override
            protected void evaluate(TaggedValuePointable[] args, IPointable result) throws SystemException {
                result.set(BOOLEAN_TRUE_CONSTANT, 0, 2);
            }
        };
    }
}