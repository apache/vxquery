package org.apache.vxquery.runtime.functions.bool;

import org.apache.vxquery.datamodel.accessors.TaggedValuePointable;
import org.apache.vxquery.datamodel.values.XDMConstants;
import org.apache.vxquery.exceptions.SystemException;
import org.apache.vxquery.runtime.functions.base.AbstractTaggedValueArgumentScalarEvaluator;
import org.apache.vxquery.runtime.functions.base.AbstractTaggedValueArgumentScalarEvaluatorFactory;

import edu.uci.ics.hyracks.algebricks.common.exceptions.AlgebricksException;
import edu.uci.ics.hyracks.algebricks.runtime.base.IScalarEvaluator;
import edu.uci.ics.hyracks.algebricks.runtime.base.IScalarEvaluatorFactory;
import edu.uci.ics.hyracks.api.context.IHyracksTaskContext;
import edu.uci.ics.hyracks.data.std.api.IPointable;
import edu.uci.ics.hyracks.data.std.primitive.BooleanPointable;

public class OrScalarEvaluatorFactory extends AbstractTaggedValueArgumentScalarEvaluatorFactory {
    private static final long serialVersionUID = 1L;

    public OrScalarEvaluatorFactory(IScalarEvaluatorFactory[] args) {
        super(args);
    }

    @Override
    protected IScalarEvaluator createEvaluator(IHyracksTaskContext ctx, IScalarEvaluator[] args)
            throws AlgebricksException {
        return new AbstractTaggedValueArgumentScalarEvaluator(args) {
            private final BooleanPointable bp = (BooleanPointable) BooleanPointable.FACTORY.createPointable();

            @Override
            protected void evaluate(TaggedValuePointable[] args, IPointable result) throws SystemException {
                for (TaggedValuePointable arg : args) {
                    arg.getValue(bp);
                    if (bp.getBoolean()) {
                        XDMConstants.setTrue(result);
                        return;
                    }
                }
                XDMConstants.setFalse(result);
            }
        };
    }
}