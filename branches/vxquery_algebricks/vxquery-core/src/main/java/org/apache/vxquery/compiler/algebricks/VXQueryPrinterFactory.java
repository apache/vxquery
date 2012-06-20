package org.apache.vxquery.compiler.algebricks;

import java.io.PrintStream;

import org.apache.vxquery.datamodel.accessors.SequencePointable;
import org.apache.vxquery.datamodel.accessors.TaggedValuePointable;
import org.apache.vxquery.datamodel.values.ValueTag;

import edu.uci.ics.hyracks.algebricks.common.exceptions.AlgebricksException;
import edu.uci.ics.hyracks.algebricks.data.IPrinter;
import edu.uci.ics.hyracks.algebricks.data.IPrinterFactory;
import edu.uci.ics.hyracks.data.std.primitive.BooleanPointable;
import edu.uci.ics.hyracks.data.std.primitive.DoublePointable;
import edu.uci.ics.hyracks.data.std.primitive.LongPointable;
import edu.uci.ics.hyracks.data.std.primitive.UTF8StringPointable;
import edu.uci.ics.hyracks.data.std.primitive.VoidPointable;

public class VXQueryPrinterFactory implements IPrinterFactory {
    private static final long serialVersionUID = 1L;

    @Override
    public IPrinter createPrinter() {
        return new IPrinter() {
            private final TaggedValuePointable tvp = new TaggedValuePointable();

            private final UTF8StringPointable utf8sp = (UTF8StringPointable) UTF8StringPointable.FACTORY
                    .createPointable();

            private final LongPointable lp = (LongPointable) LongPointable.FACTORY.createPointable();

            private final DoublePointable dp = (DoublePointable) DoublePointable.FACTORY.createPointable();

            private final BooleanPointable bp = (BooleanPointable) BooleanPointable.FACTORY.createPointable();

            private final SequencePointable seqp = new SequencePointable();

            private final VoidPointable vp = (VoidPointable) VoidPointable.FACTORY.createPointable();

            @Override
            public void print(byte[] b, int s, int l, PrintStream ps) throws AlgebricksException {
                tvp.set(b, s, l);
                byte tag = tvp.getTag();
                switch ((int) tag) {
                    case ValueTag.XS_STRING_TAG: {
                        tvp.getValue(utf8sp);
                        int utfLen = utf8sp.getUTFLen();
                        int offset = 2;
                        while (utfLen > 0) {
                            char c = utf8sp.charAt(offset);
                            ps.append(c);
                            int cLen = UTF8StringPointable.getModifiedUTF8Len(c);
                            offset += cLen;
                            utfLen -= cLen;
                        }
                        break;
                    }

                    case ValueTag.XS_INTEGER_TAG: {
                        tvp.getValue(lp);
                        ps.print(lp.longValue());
                        break;
                    }

                    case ValueTag.XS_DOUBLE_TAG: {
                        tvp.getValue(dp);
                        ps.print(dp.doubleValue());
                        break;
                    }

                    case ValueTag.XS_BOOLEAN_TAG: {
                        tvp.getValue(bp);
                        ps.print(bp.getBoolean());
                        break;
                    }

                    case ValueTag.SEQUENCE_TAG: {
                        tvp.getValue(seqp);
                        int len = seqp.getEntryCount();
                        for (int i = 0; i < len; ++i) {
                            if (i > 0) {
                                ps.append(' ');
                            }
                            seqp.getEntry(i, vp);
                            print(vp.getByteArray(), vp.getStartOffset(), vp.getLength(), ps);
                        }
                    }
                }
            }

            @Override
            public void init() throws AlgebricksException {
            }
        };
    }
}