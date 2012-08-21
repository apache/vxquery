package org.apache.vxquery.runtime.functions.type;

import org.apache.vxquery.datamodel.accessors.TaggedValuePointable;
import org.apache.vxquery.datamodel.values.XDMConstants;
import org.apache.vxquery.exceptions.SystemException;
import org.apache.vxquery.types.SequenceType;

import edu.uci.ics.hyracks.algebricks.common.exceptions.AlgebricksException;
import edu.uci.ics.hyracks.algebricks.runtime.base.IScalarEvaluator;
import edu.uci.ics.hyracks.algebricks.runtime.base.IScalarEvaluatorFactory;
import edu.uci.ics.hyracks.api.context.IHyracksTaskContext;
import edu.uci.ics.hyracks.data.std.api.IPointable;

public class InstanceOfScalarEvaluatorFactory extends AbstractTypeScalarEvaluatorFactory {
    private static final long serialVersionUID = 1L;

    public InstanceOfScalarEvaluatorFactory(IScalarEvaluatorFactory[] args) {
        super(args);
    }

    @Override
    protected IScalarEvaluator createEvaluator(IHyracksTaskContext ctx, IScalarEvaluator[] args)
            throws AlgebricksException {
        return new AbstractTypeScalarEvaluator(args, ctx) {
            private final SequenceTypeMatcher matcher = new SequenceTypeMatcher();

            @Override
            protected void evaluate(TaggedValuePointable tvp, IPointable result) throws SystemException {
                boolean success = matcher.sequenceTypeMatch(tvp);
                if (success) {
                    XDMConstants.setTrue(result);
                } else {
                    XDMConstants.setFalse(result);
                }
            }

            @Override
            protected void setSequenceType(SequenceType sType) {
                matcher.setSequenceType(sType);
            }
        };
    }
}