package org.apache.vxquery.runtime.functions.strings;

import org.apache.vxquery.datamodel.accessors.TaggedValuePointable;
import org.apache.vxquery.datamodel.values.ValueTag;
import org.apache.vxquery.exceptions.ErrorCode;
import org.apache.vxquery.exceptions.SystemException;
import org.apache.vxquery.runtime.functions.base.AbstractTaggedValueArgumentScalarEvaluator;
import org.apache.vxquery.runtime.functions.base.AbstractTaggedValueArgumentScalarEvaluatorFactory;

import edu.uci.ics.hyracks.algebricks.common.exceptions.AlgebricksException;
import edu.uci.ics.hyracks.algebricks.runtime.base.IScalarEvaluator;
import edu.uci.ics.hyracks.algebricks.runtime.base.IScalarEvaluatorFactory;
import edu.uci.ics.hyracks.data.std.api.IPointable;
import edu.uci.ics.hyracks.data.std.primitive.UTF8StringPointable;

public class FnStartsWithEvaluatorFactory extends AbstractTaggedValueArgumentScalarEvaluatorFactory {
    private static final long serialVersionUID = 1L;

    public FnStartsWithEvaluatorFactory(IScalarEvaluatorFactory[] args) {
        super(args);
    }

    @Override
    protected IScalarEvaluator createEvaluator(IScalarEvaluator[] args) throws AlgebricksException {
        final UTF8StringPointable stringp1 = new UTF8StringPointable();
        final UTF8StringPointable stringp2 = new UTF8StringPointable();
        final UTF8StringPointable stringp3 = new UTF8StringPointable();
        final ICharacterIterator charIterator1 = new UTF8StringCharacterIterator(stringp1);
        final ICharacterIterator charIterator2 = new UTF8StringCharacterIterator(stringp2);

        return new AbstractTaggedValueArgumentScalarEvaluator(args) {
            @Override
            protected void evaluate(TaggedValuePointable[] args, IPointable result) throws SystemException {
                // Default result is false.
                byte[] booleanResult = new byte[2];
                booleanResult[0] = ValueTag.XS_BOOLEAN_TAG;
                booleanResult[1] = 0;

                TaggedValuePointable tvp1 = args[0];
                TaggedValuePointable tvp2 = args[1];

                // Only accept strings as input.
                if (tvp1.getTag() != ValueTag.XS_STRING_TAG) {
                    throw new SystemException(ErrorCode.FORG0006);
                }
                if (tvp2.getTag() != ValueTag.XS_STRING_TAG) {
                    throw new SystemException(ErrorCode.FORG0006);
                }
                tvp1.getValue(stringp1);
                tvp2.getValue(stringp2);
                charIterator1.reset();
                charIterator2.reset();
                
                // Third parameter is optional.
                if (args.length > 2) {
                    TaggedValuePointable tvp3 = args[2];
                    if (tvp3.getTag() != ValueTag.XS_STRING_TAG) {
                        throw new SystemException(ErrorCode.FORG0006);
                    }
                    tvp3.getValue(stringp3);
                }
                // TODO use the third value as collation

                // Only need to run comparisons if they both have a non empty string.
                if (stringp1.getLength() > 2 && stringp2.getLength() > 2) {
                    int c1;
                    int c2;
                    while (true) {
                        c1 = charIterator1.next();
                        c2 = charIterator2.next();
                        if (ICharacterIterator.EOS_CHAR == c2) {
                            // Checked the full length of search string.
                            booleanResult[1] = 1;
                            break;
                        }
                        if (c1 != c2) {
                            // Characters do not match
                            break;
                        }
                    }
                } else if (stringp2.getLength() == 2) {
                    booleanResult[1] = 1;
                }

                result.set(booleanResult, 0, 2);
            }
        };
    }
}
