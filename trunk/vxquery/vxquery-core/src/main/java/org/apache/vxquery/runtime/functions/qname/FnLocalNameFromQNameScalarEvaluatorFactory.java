package org.apache.vxquery.runtime.functions.qname;

import java.io.DataOutput;

import org.apache.vxquery.datamodel.accessors.TaggedValuePointable;
import org.apache.vxquery.datamodel.accessors.atomic.XSQNamePointable;
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
import edu.uci.ics.hyracks.data.std.util.ArrayBackedValueStorage;

public class FnLocalNameFromQNameScalarEvaluatorFactory extends AbstractTaggedValueArgumentScalarEvaluatorFactory {
    private static final long serialVersionUID = 1L;

    public FnLocalNameFromQNameScalarEvaluatorFactory(IScalarEvaluatorFactory[] args) {
        super(args);
    }

    @Override
    protected IScalarEvaluator createEvaluator(IHyracksTaskContext ctx, IScalarEvaluator[] args)
            throws AlgebricksException {
        final XSQNamePointable qnamep = (XSQNamePointable) XSQNamePointable.FACTORY.createPointable();
        final ArrayBackedValueStorage abvs = new ArrayBackedValueStorage();
        final DataOutput dOut = abvs.getDataOutput();

        return new AbstractTaggedValueArgumentScalarEvaluator(args) {
            @Override
            protected void evaluate(TaggedValuePointable[] args, IPointable result) throws SystemException {
                TaggedValuePointable tvp1 = args[0];

                // Only accept a strings.
                if (tvp1.getTag() != ValueTag.XS_QNAME_TAG) {
                    throw new SystemException(ErrorCode.FORG0006);
                }
                tvp1.getValue(qnamep);

                try {
                    abvs.reset();
                    dOut.write(ValueTag.XS_NCNAME_TAG);
                    dOut.write(qnamep.getByteArray(),
                            qnamep.getStartOffset() + qnamep.getUriLength() + qnamep.getPrefixLength(),
                            qnamep.getLocalNameLength());
                    result.set(abvs);
                } catch (Exception e) {
                    throw new SystemException(ErrorCode.SYSE0001, e);
                }
            }
        };
    }
}