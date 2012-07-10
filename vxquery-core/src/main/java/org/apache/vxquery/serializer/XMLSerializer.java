package org.apache.vxquery.serializer;

import java.io.PrintStream;

import org.apache.vxquery.datamodel.accessors.PointablePool;
import org.apache.vxquery.datamodel.accessors.PointablePoolFactory;
import org.apache.vxquery.datamodel.accessors.SequencePointable;
import org.apache.vxquery.datamodel.accessors.TaggedValuePointable;
import org.apache.vxquery.datamodel.accessors.atomic.CodedQNamePointable;
import org.apache.vxquery.datamodel.accessors.atomic.XSDecimalPointable;
import org.apache.vxquery.datamodel.accessors.nodes.AttributeNodePointable;
import org.apache.vxquery.datamodel.accessors.nodes.DocumentNodePointable;
import org.apache.vxquery.datamodel.accessors.nodes.ElementNodePointable;
import org.apache.vxquery.datamodel.accessors.nodes.NodeTreePointable;
import org.apache.vxquery.datamodel.accessors.nodes.PINodePointable;
import org.apache.vxquery.datamodel.accessors.nodes.TextOrCommentNodePointable;
import org.apache.vxquery.datamodel.values.ValueTag;

import edu.uci.ics.hyracks.algebricks.common.exceptions.AlgebricksException;
import edu.uci.ics.hyracks.algebricks.data.IPrinter;
import edu.uci.ics.hyracks.data.std.primitive.BooleanPointable;
import edu.uci.ics.hyracks.data.std.primitive.DoublePointable;
import edu.uci.ics.hyracks.data.std.primitive.LongPointable;
import edu.uci.ics.hyracks.data.std.primitive.UTF8StringPointable;
import edu.uci.ics.hyracks.data.std.primitive.VoidPointable;

public class XMLSerializer implements IPrinter {
    private final PointablePool pp;

    private NodeTreePointable ntp;

    public XMLSerializer() {
        pp = PointablePoolFactory.INSTANCE.createPointablePool();
    }

    @Override
    public void print(byte[] b, int s, int l, PrintStream ps) {
        TaggedValuePointable tvp = pp.takeOne(TaggedValuePointable.class);
        try {
            tvp.set(b, s, l);
            printTaggedValuePointable(ps, tvp);
        } finally {
            pp.giveBack(tvp);
        }
    }

    private void printTaggedValuePointable(PrintStream ps, TaggedValuePointable tvp) {
        byte tag = tvp.getTag();
        switch ((int) tag) {
            case ValueTag.XS_STRING_TAG:
                printString(ps, tvp);
                break;

            case ValueTag.XS_UNTYPED_ATOMIC_TAG:
                printString(ps, tvp);
                break;

            case ValueTag.XS_INTEGER_TAG:
                printInteger(ps, tvp);
                break;

            case ValueTag.XS_DOUBLE_TAG:
                printDouble(ps, tvp);
                break;

            case ValueTag.XS_DECIMAL_TAG:
                printDecimal(ps, tvp);
                break;

            case ValueTag.XS_BOOLEAN_TAG:
                printBoolean(ps, tvp);
                break;

            case ValueTag.SEQUENCE_TAG:
                printSequence(ps, tvp);
                break;

            case ValueTag.NODE_TREE_TAG:
                printNodeTree(ps, tvp);
                break;

            case ValueTag.DOCUMENT_NODE_TAG:
                printDocumentNode(ps, tvp);
                break;

            case ValueTag.ELEMENT_NODE_TAG:
                printElementNode(ps, tvp);
                break;

            case ValueTag.ATTRIBUTE_NODE_TAG:
                printAttributeNode(ps, tvp);
                break;

            case ValueTag.TEXT_NODE_TAG:
                printTextNode(ps, tvp);
                break;

            case ValueTag.COMMENT_NODE_TAG:
                printCommentNode(ps, tvp);
                break;

            case ValueTag.PI_NODE_TAG:
                printPINode(ps, tvp);
                break;

            default:
                throw new UnsupportedOperationException("Encountered tag: " + tvp.getTag());
        }
    }

    private void printDecimal(PrintStream ps, TaggedValuePointable tvp) {
        XSDecimalPointable dp = pp.takeOne(XSDecimalPointable.class);
        try {
            tvp.getValue(dp);
            byte decimalPlace = dp.getDecimalPlace();
            long value = dp.getDecimalValue();
            int nDigits = (int) Math.log10(value) + 1;
            long pow10 = (long) Math.pow(10, nDigits - 1);
            int start = Math.max(decimalPlace, nDigits - 1);
            int end = Math.min(0, decimalPlace);
            if (start > nDigits) {
                ps.append("0.");
            }
            for (int i = start; i >= end; --i) {
                if (i >= nDigits || i < 0) {
                    ps.append('0');
                } else {
                    ps.append((char) ('0' + (value / pow10)));
                    value %= pow10;
                    pow10 /= 10;
                }
                if (i == decimalPlace) {
                    ps.append('.');
                }
            }
        } finally {
            pp.giveBack(dp);
        }
    }

    private void printNodeTree(PrintStream ps, TaggedValuePointable tvp) {
        ntp = pp.takeOne(NodeTreePointable.class);
        TaggedValuePointable rootTVP = pp.takeOne(TaggedValuePointable.class);
        try {
            tvp.getValue(ntp);
            ntp.getRootNode(rootTVP);
            printTaggedValuePointable(ps, rootTVP);
        } finally {
            pp.giveBack(rootTVP);
            pp.giveBack(ntp);
            ntp = null;
        }
    }

    private void printPINode(PrintStream ps, TaggedValuePointable tvp) {
        PINodePointable pnp = pp.takeOne(PINodePointable.class);
        UTF8StringPointable utf8sp = pp.takeOne(UTF8StringPointable.class);
        try {
            tvp.getValue(pnp);
            ps.append("<?");
            pnp.getTarget(ntp, utf8sp);
            printString(ps, utf8sp);
            ps.append(' ');
            pnp.getContent(ntp, utf8sp);
            printString(ps, utf8sp);
            ps.append("?>");
        } finally {
            pp.giveBack(pnp);
            pp.giveBack(utf8sp);
        }
    }

    private void printCommentNode(PrintStream ps, TaggedValuePointable tvp) {
        TextOrCommentNodePointable tcnp = pp.takeOne(TextOrCommentNodePointable.class);
        UTF8StringPointable utf8sp = pp.takeOne(UTF8StringPointable.class);
        try {
            tvp.getValue(tcnp);
            tcnp.getValue(ntp, utf8sp);
            ps.append("<!--");
            printString(ps, utf8sp);
            ps.append("-->");
        } finally {
            pp.giveBack(tcnp);
            pp.giveBack(utf8sp);
        }
    }

    private void printTextNode(PrintStream ps, TaggedValuePointable tvp) {
        TextOrCommentNodePointable tcnp = pp.takeOne(TextOrCommentNodePointable.class);
        UTF8StringPointable utf8sp = pp.takeOne(UTF8StringPointable.class);
        try {
            tvp.getValue(tcnp);
            tcnp.getValue(ntp, utf8sp);
            printString(ps, utf8sp);
        } finally {
            pp.giveBack(tcnp);
            pp.giveBack(utf8sp);
        }
    }

    private void printAttributeNode(PrintStream ps, TaggedValuePointable tvp) {
        AttributeNodePointable anp = pp.takeOne(AttributeNodePointable.class);
        CodedQNamePointable cqp = pp.takeOne(CodedQNamePointable.class);
        UTF8StringPointable utf8sp = pp.takeOne(UTF8StringPointable.class);
        TaggedValuePointable valueTVP = pp.takeOne(TaggedValuePointable.class);
        try {
            tvp.getValue(anp);
            anp.getName(cqp);
            printPrefixedQName(ps, cqp, utf8sp);
            ps.append("=\"");
            anp.getValue(ntp, valueTVP);
            printTaggedValuePointable(ps, valueTVP);
            ps.append('"');
        } finally {
            pp.giveBack(valueTVP);
            pp.giveBack(utf8sp);
            pp.giveBack(anp);
            pp.giveBack(cqp);
        }
    }

    private void printElementNode(PrintStream ps, TaggedValuePointable tvp) {
        ElementNodePointable enp = pp.takeOne(ElementNodePointable.class);
        CodedQNamePointable cqp = pp.takeOne(CodedQNamePointable.class);
        UTF8StringPointable utf8sp = pp.takeOne(UTF8StringPointable.class);
        SequencePointable seqp = pp.takeOne(SequencePointable.class);
        try {
            tvp.getValue(enp);
            enp.getName(cqp);
            ps.append('<');
            printPrefixedQName(ps, cqp, utf8sp);

            int nsCount = enp.getNamespaceEntryCount(ntp);
            for (int i = 0; i < nsCount; ++i) {
                ps.append(" xmlns:");
                ntp.getString(enp.getNamespacePrefixCode(ntp, i), utf8sp);
                printString(ps, utf8sp);
                ps.append("=\"");
                ntp.getString(enp.getNamespaceURICode(ntp, i), utf8sp);
                printString(ps, utf8sp);
                ps.append("\"");
            }

            enp.getAttributeSequence(ntp, seqp);
            if (seqp.getByteArray() != null && seqp.getEntryCount() > 0) {
                ps.append(' ');
                printSequence(ps, seqp);
            }

            ps.append('>');
            enp.getChildrenSequence(ntp, seqp);
            if (seqp.getByteArray() != null) {
                printSequence(ps, seqp);
            }
            ps.append("</");
            printPrefixedQName(ps, cqp, utf8sp);
            ps.append('>');
        } finally {
            pp.giveBack(seqp);
            pp.giveBack(utf8sp);
            pp.giveBack(cqp);
            pp.giveBack(enp);
        }
    }

    private void printPrefixedQName(PrintStream ps, CodedQNamePointable cqp, UTF8StringPointable utf8sp) {
        ntp.getString(cqp.getPrefixCode(), utf8sp);
        if (utf8sp.getStringLength() > 0) {
            printString(ps, utf8sp);
            ps.append(':');
        }
        ntp.getString(cqp.getLocalCode(), utf8sp);
        printString(ps, utf8sp);
    }

    private void printDocumentNode(PrintStream ps, TaggedValuePointable tvp) {
        DocumentNodePointable dnp = pp.takeOne(DocumentNodePointable.class);
        SequencePointable seqp = pp.takeOne(SequencePointable.class);
        try {
            ps.append("<?xml version=\"1.0\"?>\n");
            tvp.getValue(dnp);
            dnp.getContent(ntp, seqp);
            printSequence(ps, seqp);
        } finally {
            pp.giveBack(dnp);
        }
    }

    private void printSequence(PrintStream ps, TaggedValuePointable tvp) {
        SequencePointable seqp = pp.takeOne(SequencePointable.class);
        try {
            printSequence(ps, seqp);
        } finally {
            pp.giveBack(seqp);
        }
    }

    private void printSequence(PrintStream ps, SequencePointable seqp) {
        VoidPointable vp = pp.takeOne(VoidPointable.class);
        try {
            int len = seqp.getEntryCount();
            for (int i = 0; i < len; ++i) {
                if (i > 0) {
                    ps.append(' ');
                }
                seqp.getEntry(i, vp);
                print(vp.getByteArray(), vp.getStartOffset(), vp.getLength(), ps);
            }
        } finally {
            pp.giveBack(vp);
        }
    }

    private void printBoolean(PrintStream ps, TaggedValuePointable tvp) {
        BooleanPointable bp = pp.takeOne(BooleanPointable.class);
        try {
            tvp.getValue(bp);
            ps.print(bp.getBoolean());
        } finally {
            pp.giveBack(bp);
        }
    }

    private void printDouble(PrintStream ps, TaggedValuePointable tvp) {
        DoublePointable dp = pp.takeOne(DoublePointable.class);
        try {
            tvp.getValue(dp);
            ps.print(dp.doubleValue());
        } finally {
            pp.giveBack(dp);
        }
    }

    private void printInteger(PrintStream ps, TaggedValuePointable tvp) {
        LongPointable lp = pp.takeOne(LongPointable.class);
        try {
            tvp.getValue(lp);
            ps.print(lp.longValue());
        } finally {
            pp.giveBack(lp);
        }
    }

    private void printString(PrintStream ps, TaggedValuePointable tvp) {
        UTF8StringPointable utf8sp = pp.takeOne(UTF8StringPointable.class);
        try {
            tvp.getValue(utf8sp);
            printString(ps, utf8sp);
        } finally {
            pp.giveBack(utf8sp);
        }
    }

    private void printString(PrintStream ps, UTF8StringPointable utf8sp) {
        int utfLen = utf8sp.getUTFLength();
        int offset = 2;
        while (utfLen > 0) {
            char c = utf8sp.charAt(offset);
            switch (c) {
                case '<':
                    ps.append("&lt;");
                    break;

                case '>':
                    ps.append("&gt;");
                    break;

                case '&':
                    ps.append("&amp;");
                    break;

                case '"':
                    ps.append("&quot;");
                    break;

                case '\'':
                    ps.append("&apos;");
                    break;

                default:
                    ps.append(c);
                    break;
            }
            int cLen = UTF8StringPointable.getModifiedUTF8Len(c);
            offset += cLen;
            utfLen -= cLen;
        }
    }

    @Override
    public void init() throws AlgebricksException {
    }
}