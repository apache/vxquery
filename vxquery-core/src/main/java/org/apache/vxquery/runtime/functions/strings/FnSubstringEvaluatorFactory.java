package org.apache.vxquery.runtime.functions.strings;

import org.apache.vxquery.datamodel.accessors.TaggedValuePointable;
import org.apache.vxquery.datamodel.values.ValueTag;
import org.apache.vxquery.exceptions.ErrorCode;
import org.apache.vxquery.exceptions.SystemException;

import edu.uci.ics.hyracks.algebricks.common.exceptions.AlgebricksException;
import edu.uci.ics.hyracks.algebricks.runtime.base.IScalarEvaluator;
import edu.uci.ics.hyracks.algebricks.runtime.base.IScalarEvaluatorFactory;
import edu.uci.ics.hyracks.api.context.IHyracksTaskContext;
import edu.uci.ics.hyracks.data.std.primitive.DoublePointable;
import edu.uci.ics.hyracks.data.std.primitive.LongPointable;
import edu.uci.ics.hyracks.data.std.primitive.UTF8StringPointable;
import edu.uci.ics.hyracks.data.std.util.ArrayBackedValueStorage;

public class FnSubstringEvaluatorFactory extends AbstractCharacterIteratorCopyingEvaluatorFactory {
    private static final long serialVersionUID = 1L;

    public FnSubstringEvaluatorFactory(IScalarEvaluatorFactory[] args) {
        super(args);
    }

    protected static int getIntParameter(final TaggedValuePointable tvp, final DoublePointable doublep,
            final LongPointable longp) throws SystemException {
        switch (tvp.getTag()) {
            case ValueTag.XS_INTEGER_TAG:
                tvp.getValue(longp);
                return longp.intValue();
            case ValueTag.XS_DOUBLE_TAG:
                tvp.getValue(doublep);
                // TODO Double needs to be rounded
                return doublep.intValue();
            default:
                throw new SystemException(ErrorCode.FORG0006);
        }
    }

    @Override
    protected IScalarEvaluator createEvaluator(IHyracksTaskContext ctx, IScalarEvaluator[] args)
            throws AlgebricksException {
        final UTF8StringPointable stringp = (UTF8StringPointable) UTF8StringPointable.FACTORY.createPointable();
        final DoublePointable doublep = (DoublePointable) DoublePointable.FACTORY.createPointable();
        final LongPointable longp = (LongPointable) LongPointable.FACTORY.createPointable();
        final ArrayBackedValueStorage abvs = new ArrayBackedValueStorage();
        final SubstringCharacterIterator charIterator = new SubstringCharacterIterator(new UTF8StringCharacterIterator(
                stringp));

        return new AbstractCharacterIteratorCopyingEvaluator(args, charIterator) {
            @Override
            protected void preEvaluate(TaggedValuePointable[] args) throws SystemException {
                int startingLocation = 1;
                int length = Integer.MAX_VALUE;
                abvs.reset();
                charIterator.reset();

                // Only accept string, double, and optional double as input.
                TaggedValuePointable tvp1 = args[0];
                if (tvp1.getTag() != ValueTag.XS_STRING_TAG) {
                    throw new SystemException(ErrorCode.FORG0006);
                }
                tvp1.getValue(stringp);

                // TODO Check specification to see if only double? If so change passing function.
                startingLocation = getIntParameter(args[1], doublep, longp);

                // Third parameter may override default endingLoc.
                if (args.length > 2) {
                    length = getIntParameter(args[2], doublep, longp);
                }

                charIterator.setBounds(startingLocation, length);
            }
        };
    }
}
