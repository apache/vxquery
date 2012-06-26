package org.apache.vxquery.runtime.functions.strings;

import java.io.DataOutput;
import java.io.IOException;

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
import edu.uci.ics.hyracks.data.std.primitive.DoublePointable;
import edu.uci.ics.hyracks.data.std.primitive.LongPointable;
import edu.uci.ics.hyracks.data.std.primitive.UTF8StringPointable;
import edu.uci.ics.hyracks.dataflow.common.data.accessors.ArrayBackedValueStorage;

public class FnSubstringEvaluatorFactory extends AbstractTaggedValueArgumentScalarEvaluatorFactory {
    private static final long serialVersionUID = 1L;

    public FnSubstringEvaluatorFactory(IScalarEvaluatorFactory[] args) {
        super(args);
    }

    @Override
    protected IScalarEvaluator createEvaluator(IScalarEvaluator[] args) throws AlgebricksException {
        final UTF8StringPointable stringp = new UTF8StringPointable();
        final DoublePointable doublep1 = new DoublePointable();
        final DoublePointable doublep2 = new DoublePointable();
        final LongPointable longp1 = new LongPointable();
        final LongPointable longp2 = new LongPointable();
        final ArrayBackedValueStorage abvs = new ArrayBackedValueStorage();

        return new AbstractTaggedValueArgumentScalarEvaluator(args) {
            @Override
            protected void evaluate(TaggedValuePointable[] args, IPointable result) throws SystemException {
                int startingLoc = 0;
                int endingLoc;
                byte[] stringResult;

                abvs.reset();
                TaggedValuePointable tvp1 = args[0];
                TaggedValuePointable tvp2 = args[1];

                // Only accept string, double, and optional double as input.
                if (tvp1.getTag() != ValueTag.XS_STRING_TAG) {
                    throw new SystemException(ErrorCode.FORG0006);
                }
                tvp1.getValue(stringp);
                int length = stringp.getStringLength();

                // TODO Check specification to see if only double? If so change passing function.
                switch (tvp2.getTag()) {
                    case ValueTag.XS_INTEGER_TAG:
                        tvp2.getValue(longp1);
                        startingLoc = longp1.intValue();
                        break;
                    case ValueTag.XS_DOUBLE_TAG:
                        tvp2.getValue(doublep1);
                        // TODO Double needs to be rounded
                        startingLoc = doublep1.intValue();
                        break;
                    default:
                        throw new SystemException(ErrorCode.FORG0006);
                }

                // Third parameter may override default endingLoc.
                if (args.length > 2) {
                    TaggedValuePointable tvp3 = args[2];
                    int lengthp = 0;
                    // TODO Check specification to see if only double? If so change passing function.
                    switch (tvp3.getTag()) {
                        case ValueTag.XS_INTEGER_TAG:
                            tvp3.getValue(longp2);
                            lengthp = longp2.intValue();
                            break;
                        case ValueTag.XS_DOUBLE_TAG:
                            tvp3.getValue(doublep2);
                            // TODO Double needs to be rounded
                            lengthp = doublep2.intValue();
                            break;
                        default:
                            throw new SystemException(ErrorCode.FORG0006);
                    }
                    tvp3.getValue(doublep2);
                    endingLoc = startingLoc + lengthp;
                } else {
                    endingLoc = length;
                }

                try {
                    // Byte Format: Type (1 byte) + String Length (2 bytes) + String.
                    DataOutput out = abvs.getDataOutput();
                    out.write(ValueTag.XS_STRING_TAG);

                    // Default length to zero and update later if needed.
                    out.write(0xFF);
                    out.write(0xFF);

                    // Only check if the section is logically in the string.
                    if (endingLoc > startingLoc) {
                        int c = 0;
                        int i = 0;
                        int offset = 2;
                        for (; i < endingLoc && i < length; ++i) {
                            c = stringp.charAt(offset);
                            if (!((c >= 0x0001) && (c <= 0x007F))) {
                                break;
                            }
                            if (i >= startingLoc) {
                                out.write((byte) c);
                            }
                            ++offset;
                        }

                        // Must look at full range of characters.
                        for (; i < endingLoc && i < length; ++i) {
                            c = stringp.charAt(offset);
                            if ((c >= 0x0001) && (c <= 0x007F)) {
                                if (i >= startingLoc) {
                                    out.write((byte) c);
                                }
                                ++offset;
                            } else if (c > 0x07FF) {
                                if (i >= startingLoc) {
                                    out.write((byte) (0xE0 | ((c >> 12) & 0x0F)));
                                    out.write((byte) (0x80 | ((c >> 6) & 0x3F)));
                                    out.write((byte) (0x80 | ((c >> 0) & 0x3F)));
                                }
                                offset += 3;
                            } else {
                                if (i >= startingLoc) {
                                    out.write((byte) (0xC0 | ((c >> 6) & 0x1F)));
                                    out.write((byte) (0x80 | ((c >> 0) & 0x3F)));
                                }
                                offset += 2;
                            }
                        }

                        // Update the full length string in the byte array.
                        stringResult = abvs.getByteArray();
                        stringResult[1] = (byte) (((abvs.getLength() - 3) >>> 8) & 0xFF);
                        stringResult[2] = (byte) (((abvs.getLength() - 3) >>> 0) & 0xFF);

                        result.set(stringResult, 0, abvs.getLength());
                    } else {
                        // Return an empty string;
                        result.set(abvs.getByteArray(), 0, abvs.getLength());
                    }
                } catch (IOException e) {
                    throw new SystemException(ErrorCode.SYSE0001, e);
                }
            }
        };
    }
}
