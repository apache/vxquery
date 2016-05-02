package org.apache.vxquery.index;

import java.io.DataOutput;
import java.io.IOException;
import java.io.PrintStream;
import java.util.Collections;
import java.util.Comparator;
import java.util.Vector;

import org.apache.commons.lang3.StringUtils;
import org.apache.lucene.document.Document;
import org.apache.lucene.document.Field;
import org.apache.lucene.document.StringField;
import org.apache.lucene.index.IndexWriter;
import org.apache.vxquery.datamodel.accessors.PointablePool;
import org.apache.vxquery.datamodel.accessors.PointablePoolFactory;
import org.apache.vxquery.datamodel.accessors.SequencePointable;
import org.apache.vxquery.datamodel.accessors.TaggedValuePointable;
import org.apache.vxquery.datamodel.accessors.atomic.CodedQNamePointable;
import org.apache.vxquery.datamodel.accessors.atomic.XSBinaryPointable;
import org.apache.vxquery.datamodel.accessors.atomic.XSDatePointable;
import org.apache.vxquery.datamodel.accessors.atomic.XSDateTimePointable;
import org.apache.vxquery.datamodel.accessors.atomic.XSDecimalPointable;
import org.apache.vxquery.datamodel.accessors.atomic.XSDurationPointable;
import org.apache.vxquery.datamodel.accessors.atomic.XSQNamePointable;
import org.apache.vxquery.datamodel.accessors.atomic.XSTimePointable;
import org.apache.vxquery.datamodel.accessors.nodes.AttributeNodePointable;
import org.apache.vxquery.datamodel.accessors.nodes.DocumentNodePointable;
import org.apache.vxquery.datamodel.accessors.nodes.ElementNodePointable;
import org.apache.vxquery.datamodel.accessors.nodes.NodeTreePointable;
import org.apache.vxquery.datamodel.accessors.nodes.PINodePointable;
import org.apache.vxquery.datamodel.accessors.nodes.TextOrCommentNodePointable;
import org.apache.vxquery.datamodel.values.ValueTag;
import org.apache.vxquery.exceptions.SystemException;
import org.apache.vxquery.runtime.functions.cast.CastToStringOperation;

import edu.uci.ics.hyracks.data.std.api.IPointable;
import edu.uci.ics.hyracks.data.std.primitive.BooleanPointable;
import edu.uci.ics.hyracks.data.std.primitive.BytePointable;
import edu.uci.ics.hyracks.data.std.primitive.DoublePointable;
import edu.uci.ics.hyracks.data.std.primitive.FloatPointable;
import edu.uci.ics.hyracks.data.std.primitive.IntegerPointable;
import edu.uci.ics.hyracks.data.std.primitive.LongPointable;
import edu.uci.ics.hyracks.data.std.primitive.ShortPointable;
import edu.uci.ics.hyracks.data.std.primitive.UTF8StringPointable;
import edu.uci.ics.hyracks.data.std.primitive.VoidPointable;
import edu.uci.ics.hyracks.data.std.util.ArrayBackedValueStorage;

public class IndexBuilderDoc {
    private IPointable treepointable;
    private PrintStream ps = System.out;

    private PointablePool pp;
    private NodeTreePointable ntp;

    private ArrayBackedValueStorage abvs = new ArrayBackedValueStorage();
    private DataOutput dOut = abvs.getDataOutput();
    private CastToStringOperation castToString = new CastToStringOperation();
    private Document doc;
    private Vector<ComplexItem> results;

    private byte[] bstart;
    private int sstart;
    private int lstart;

    String filepath;
    IndexWriter writer;
    class ComplexItem{
        public StringField sf;
        public String id;
        public ComplexItem(StringField sfin, String idin){
            sf = sfin;
            id = idin;
        }
    }
    class idcomparitor implements Comparator{

        @Override
        public int compare(Object o1, Object o2) {
            String a = ((ComplexItem)o1).id;
            String b = ((ComplexItem)o2).id;
            String[] apieces = a.split("\\.");
            String[] bpieces = b.split("\\.");
            for (int i = 0; i < Math.min(apieces.length, bpieces.length); i++){
                int numa = Integer.parseInt(apieces[i]);
                int numb = Integer.parseInt(bpieces[i]);
                if (numa > numb){
                    return 1;
                }
                if (numa < numb){
                    return -1;
                }
                
            }
            if (apieces.length > bpieces.length){
                return 1;
            }
            return -1;
        }
    }

    public IndexBuilderDoc(IPointable tree, IndexWriter inwriter, String infilepath) {
        this.treepointable = tree;
        writer = inwriter;
        filepath = infilepath;

        //convert to tagged value pointable
        TaggedValuePointable tvp = (TaggedValuePointable) TaggedValuePointable.FACTORY.createPointable();
        tvp.set(treepointable.getByteArray(), 0, treepointable.getLength());

        //get bytes and info from doc pointer
        bstart = tvp.getByteArray();
        sstart = tvp.getStartOffset();
        lstart = tvp.getLength();
        
        doc = new Document();
        
        results = new Vector();

        pp = PointablePoolFactory.INSTANCE.createPointablePool();
    }

    //This is a wrapper to start indexing using the functions adapted from XMLSerializer
    public void printstart() {

        print(bstart, sstart, lstart, ps, "0", "");
        Collections.sort(results,new idcomparitor());
        for (int i = 2; i < results.size()-1; i++){
            doc.add(results.elementAt(i).sf);  
        }
        try {
            writer.addDocument(doc);
        } catch (IOException e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
        }
        System.out.println(doc.toString());

    }

    //adapted from XMLSerializer. The following functions are used to traverse the TaggedValuePointable
    //and create the index elements, then add them to the lucene index.
    public void print(byte[] b, int s, int l, PrintStream ps, String myid, String epath) {
        TaggedValuePointable tvp = pp.takeOne(TaggedValuePointable.class);
        try {
            tvp.set(b, s, l);
            printTaggedValuePointable(ps, tvp, myid, epath);
        } finally {
            pp.giveBack(tvp);
        }
    }

    private void printTaggedValuePointable(PrintStream ps, TaggedValuePointable tvp, String myid, String epath) {
        byte tag = tvp.getTag();
        String type = "text";
        String parentpath = epath;
        String[] result = { "", "" };
        switch ((int) tag) {
            case ValueTag.XS_ANY_URI_TAG:
                result = printString(ps, tvp, myid, epath);
                break;

            case ValueTag.XS_BASE64_BINARY_TAG:
                result = printBase64Binary(ps, tvp, myid, epath);
                break;

            case ValueTag.XS_BOOLEAN_TAG:
                result = printBoolean(ps, tvp, myid, epath);
                break;

            case ValueTag.XS_DATE_TAG:
                result = printDate(ps, tvp, myid, epath);
                break;

            case ValueTag.XS_DATETIME_TAG:
                result = printDateTime(ps, tvp, myid, epath);
                break;

            case ValueTag.XS_DAY_TIME_DURATION_TAG:
                result = printDTDuration(ps, tvp, myid, epath);
                break;

            case ValueTag.XS_BYTE_TAG:
                result = printByte(ps, tvp, myid, epath);
                break;

            case ValueTag.XS_DECIMAL_TAG:
                result = printDecimal(ps, tvp, myid, epath);
                break;

            case ValueTag.XS_DOUBLE_TAG:
                result = printDouble(ps, tvp, myid, epath);
                break;

            case ValueTag.XS_DURATION_TAG:
                result = printDuration(ps, tvp, myid, epath);
                break;

            case ValueTag.XS_FLOAT_TAG:
                result = printFloat(ps, tvp, myid, epath);
                break;

            case ValueTag.XS_G_DAY_TAG:
                result = printGDay(ps, tvp, myid, epath);
                break;

            case ValueTag.XS_G_MONTH_TAG:
                result = printGMonth(ps, tvp, myid, epath);
                break;

            case ValueTag.XS_G_MONTH_DAY_TAG:
                result = printGMonthDay(ps, tvp, myid, epath);
                break;

            case ValueTag.XS_G_YEAR_TAG:
                result = printGYear(ps, tvp, myid, epath);
                break;

            case ValueTag.XS_G_YEAR_MONTH_TAG:
                result = printGYearMonth(ps, tvp, myid, epath);
                break;

            case ValueTag.XS_HEX_BINARY_TAG:
                result = printHexBinary(ps, tvp, myid, epath);
                break;

            case ValueTag.XS_INT_TAG:
            case ValueTag.XS_UNSIGNED_SHORT_TAG:
                result = printInt(ps, tvp, myid, epath);
                break;

            case ValueTag.XS_INTEGER_TAG:
            case ValueTag.XS_LONG_TAG:
            case ValueTag.XS_NEGATIVE_INTEGER_TAG:
            case ValueTag.XS_NON_POSITIVE_INTEGER_TAG:
            case ValueTag.XS_NON_NEGATIVE_INTEGER_TAG:
            case ValueTag.XS_POSITIVE_INTEGER_TAG:
            case ValueTag.XS_UNSIGNED_INT_TAG:
            case ValueTag.XS_UNSIGNED_LONG_TAG:
                result = printInteger(ps, tvp, myid, epath);
                break;

            case ValueTag.XS_NOTATION_TAG:
                result = printString(ps, tvp, myid, epath);
                break;

            case ValueTag.XS_QNAME_TAG:
                result = printQName(ps, tvp, myid, epath);
                break;

            case ValueTag.XS_SHORT_TAG:
            case ValueTag.XS_UNSIGNED_BYTE_TAG:
                result = printShort(ps, tvp, myid, epath);
                break;

            case ValueTag.XS_STRING_TAG:
            case ValueTag.XS_NORMALIZED_STRING_TAG:
            case ValueTag.XS_TOKEN_TAG:
            case ValueTag.XS_LANGUAGE_TAG:
            case ValueTag.XS_NMTOKEN_TAG:
            case ValueTag.XS_NAME_TAG:
            case ValueTag.XS_NCNAME_TAG:
            case ValueTag.XS_ID_TAG:
            case ValueTag.XS_IDREF_TAG:
            case ValueTag.XS_ENTITY_TAG:
                result = printString(ps, tvp, myid, epath);
                break;

            case ValueTag.XS_TIME_TAG:
                result = printTime(ps, tvp, myid, epath);
                break;

            case ValueTag.XS_UNTYPED_ATOMIC_TAG:
                result = printString(ps, tvp, myid, epath);
                break;

            case ValueTag.XS_YEAR_MONTH_DURATION_TAG:
                result = printYMDuration(ps, tvp, myid, epath);
                break;

            case ValueTag.ATTRIBUTE_NODE_TAG:
                type = "attribute";
                result = printAttributeNode(ps, tvp, myid, epath);
                break;

            case ValueTag.TEXT_NODE_TAG:
                type = "textnode";
                result = printTextNode(ps, tvp, myid, epath);
                break;

            case ValueTag.COMMENT_NODE_TAG:
                type = "comment";
                result = printCommentNode(ps, tvp, myid, epath);
                break;

            case ValueTag.SEQUENCE_TAG:
                type = "sequence";
                printSequence(ps, tvp, myid, epath);
                break;

            case ValueTag.NODE_TREE_TAG:
                type = "tree";
                printNodeTree(ps, tvp, myid, epath);
                break;

            case ValueTag.PI_NODE_TAG:
                type = "PI";
                printPINode(ps, tvp, myid, epath);
                break;
                
            case ValueTag.ELEMENT_NODE_TAG:
                type = "element";
                result = printElementNode(ps, tvp, myid, epath);
                break;
                
            case ValueTag.DOCUMENT_NODE_TAG:
                type = "doc";
                //Create an Indexelement
                IndexElement test = new IndexElement(result[0], myid, type, result[1]);

                String ezpath = test.epath();
                //Parser doesn't like / so paths are saved as name.name....
                String betterepath = ezpath.replaceAll("/", ".");

                //Add this element to the array (they will be added in reverse order.
                String fullitem = betterepath + "."+ test.type();
            

                results.add(new ComplexItem(new StringField("item", fullitem, Field.Store.YES), test.id()));
                printDocumentNode(ps, tvp, myid, epath);
                break;

            default:
                throw new UnsupportedOperationException("Encountered tag: " + tvp.getTag());
        }
        if (!type.equals("doc")){
            //Create an Indexelement
            IndexElement test = new IndexElement(result[0], myid, type, result[1]);

            String ezpath = test.epath();
            //Parser doesn't like / so paths are saved as name.name....
            ezpath = StringUtils.replace(ezpath, parentpath, "");
            String betterepath = ezpath.replaceFirst("/", ":");
            parentpath = parentpath.replaceAll("/", ".");
          

            //Add this element to the array (they will be added in reverse order.
            String fullitem = parentpath + betterepath + "."+ test.type();
        

            results.add(new ComplexItem(new StringField("item", fullitem, Field.Store.YES), test.id()));
        }

    }

    private String[] printDecimal(PrintStream ps, TaggedValuePointable tvp, String myid, String epath) {
        String[] result = { "", epath };
        XSDecimalPointable dp = pp.takeOne(XSDecimalPointable.class);
        try {
            tvp.getValue(dp);
            abvs.reset();
            castToString.convertDecimal(dp, dOut);
            result = printStringAbvs(ps, myid, epath);
        } catch (SystemException e) {
            e.printStackTrace();
        } catch (IOException e) {
            e.printStackTrace();
        } finally {
            pp.giveBack(dp);
        }
        return result;
    }

    private void printNodeTree(PrintStream ps, TaggedValuePointable tvp, String myid, String epath) {
        if (ntp != null) {
            throw new IllegalStateException("Nested NodeTreePointable found");
        }
        ntp = pp.takeOne(NodeTreePointable.class);
        TaggedValuePointable rootTVP = pp.takeOne(TaggedValuePointable.class);
        try {
            tvp.getValue(ntp);
            ntp.getRootNode(rootTVP);
            printTaggedValuePointable(ps, rootTVP, myid, epath);
        } finally {
            pp.giveBack(rootTVP);
            pp.giveBack(ntp);
            ntp = null;
        }
    }

    private void printPINode(PrintStream ps, TaggedValuePointable tvp, String myid, String epath) {
        PINodePointable pnp = pp.takeOne(PINodePointable.class);
        UTF8StringPointable utf8sp = pp.takeOne(UTF8StringPointable.class);
        try {
            tvp.getValue(pnp);
            ps.append("<?");
            pnp.getTarget(ntp, utf8sp);
            printString(ps, utf8sp, myid, epath);
            ps.append(' ');
            pnp.getContent(ntp, utf8sp);
            printString(ps, utf8sp, myid, epath);
            ps.append("?>");
        } finally {
            pp.giveBack(pnp);
            pp.giveBack(utf8sp);
        }
    }

    private String[] printCommentNode(PrintStream ps, TaggedValuePointable tvp, String myid, String epath) {
        String[] result = { "", epath };
        TextOrCommentNodePointable tcnp = pp.takeOne(TextOrCommentNodePointable.class);
        UTF8StringPointable utf8sp = pp.takeOne(UTF8StringPointable.class);
        try {
            tvp.getValue(tcnp);
            tcnp.getValue(ntp, utf8sp);

            result = printString(ps, utf8sp, myid, epath);

        } finally {
            pp.giveBack(tcnp);
            pp.giveBack(utf8sp);
        }
        return result;
    }

    private String[] printTextNode(PrintStream ps, TaggedValuePointable tvp, String myid, String epath) {
        String[] result = { "", epath };
        TextOrCommentNodePointable tcnp = pp.takeOne(TextOrCommentNodePointable.class);
        UTF8StringPointable utf8sp = pp.takeOne(UTF8StringPointable.class);
        try {
            tvp.getValue(tcnp);
            tcnp.getValue(ntp, utf8sp);
            result = printString(ps, utf8sp, myid, epath);
        } finally {
            pp.giveBack(tcnp);
            pp.giveBack(utf8sp);
        }
        return result;
    }

    private String[] printAttributeNode(PrintStream ps, TaggedValuePointable tvp, String myid, String epath) {
        String[] result = { "", epath };
        AttributeNodePointable anp = pp.takeOne(AttributeNodePointable.class);
        CodedQNamePointable cqp = pp.takeOne(CodedQNamePointable.class);
        UTF8StringPointable utf8sp = pp.takeOne(UTF8StringPointable.class);
        TaggedValuePointable valueTVP = pp.takeOne(TaggedValuePointable.class);
        try {
            tvp.getValue(anp);
            anp.getName(cqp);
            result = printPrefixedQName(ps, cqp, utf8sp, myid, epath);

            anp.getValue(ntp, valueTVP);

            String attributevalueid = myid + ".0";
            printTaggedValuePointable(ps, valueTVP, attributevalueid, result[1]);

        } finally {
            pp.giveBack(valueTVP);
            pp.giveBack(utf8sp);
            pp.giveBack(anp);
            pp.giveBack(cqp);
        }
        return result;
    }

    private String[] printElementNode(PrintStream ps, TaggedValuePointable tvp, String myid, String epath) {
        String[] result = { "", epath };
        ElementNodePointable enp = pp.takeOne(ElementNodePointable.class);
        CodedQNamePointable cqp = pp.takeOne(CodedQNamePointable.class);
        UTF8StringPointable utf8sp = pp.takeOne(UTF8StringPointable.class);
        SequencePointable seqp = pp.takeOne(SequencePointable.class);
        try {
            tvp.getValue(enp);
            enp.getName(cqp);
            result = printPrefixedQName(ps, cqp, utf8sp, myid, epath);

            int nsCount = enp.getNamespaceEntryCount(ntp);
            for (int i = 0; i < nsCount; ++i) {
                ps.append(" xmlns:");
                ntp.getString(enp.getNamespacePrefixCode(ntp, i), utf8sp);
                printString(ps, utf8sp, myid, epath);
                ps.append("=\"");
                ntp.getString(enp.getNamespaceURICode(ntp, i), utf8sp);
                printString(ps, utf8sp, myid, epath);
                ps.append("\"");
            }

            enp.getAttributeSequence(ntp, seqp);
            int numattributes = 0;
            if (seqp.getByteArray() != null && seqp.getEntryCount() > 0) {
                ps.append(' ');
                printSequence(ps, seqp, myid, 0, result[1]);
                numattributes = seqp.getEntryCount();
            }

            enp.getChildrenSequence(ntp, seqp);
            if (seqp.getByteArray() != null) {
                printSequence(ps, seqp, myid, numattributes, result[1]);
            }

        } finally {
            pp.giveBack(seqp);
            pp.giveBack(utf8sp);
            pp.giveBack(cqp);
            pp.giveBack(enp);
        }
        return result;
    }

    private String[] printPrefixedQName(PrintStream ps, CodedQNamePointable cqp, UTF8StringPointable utf8sp,
            String myid, String epath) {
        ntp.getString(cqp.getPrefixCode(), utf8sp);
        if (utf8sp.getStringLength() > 0) {
            printString(ps, utf8sp, myid, epath);
            ps.append(':');
        }
        ntp.getString(cqp.getLocalCode(), utf8sp);
        return printString(ps, utf8sp, myid, epath);
    }

    private void printDocumentNode(PrintStream ps, TaggedValuePointable tvp, String myid, String epath) {
        DocumentNodePointable dnp = pp.takeOne(DocumentNodePointable.class);
        SequencePointable seqp = pp.takeOne(SequencePointable.class);
        try {
            tvp.getValue(dnp);
            dnp.getContent(ntp, seqp);
            printSequence(ps, seqp, myid, 0, epath);
        } finally {
            pp.giveBack(seqp);
            pp.giveBack(dnp);
        }
    }

    private void printSequence(PrintStream ps, TaggedValuePointable tvp, String myid, String epath) {
        SequencePointable seqp = pp.takeOne(SequencePointable.class);
        try {
            tvp.getValue(seqp);
            printSequence(ps, seqp, myid, 0, epath);
        } finally {
            pp.giveBack(seqp);
        }
    }

    private void printSequence(PrintStream ps, SequencePointable seqp, String myid, int addon, String epath) {
        VoidPointable vp = pp.takeOne(VoidPointable.class);
        try {
            int len = seqp.getEntryCount();
            for (int i = 0; i < len; ++i) {
                int location = i + addon;
                String childid = myid + "." + Integer.toString(location);
                seqp.getEntry(i, vp);
                print(vp.getByteArray(), vp.getStartOffset(), vp.getLength(), ps, childid, epath);
            }
        } finally {
            pp.giveBack(vp);
        }
    }

    private String[] printBase64Binary(PrintStream ps, TaggedValuePointable tvp, String myid, String epath) {
        String[] result = { "", epath };
        XSBinaryPointable bp = pp.takeOne(XSBinaryPointable.class);
        try {
            tvp.getValue(bp);
            abvs.reset();
            castToString.convertBase64Binary(bp, dOut);
            result = printStringAbvs(ps, myid, epath);
        } catch (SystemException e) {
            e.printStackTrace();
        } catch (IOException e) {
            e.printStackTrace();
        } finally {
            pp.giveBack(bp);
        }
        return result;
    }

    private String[] printBoolean(PrintStream ps, TaggedValuePointable tvp, String myid, String epath) {
        String[] result = { "", epath };
        BooleanPointable bp = pp.takeOne(BooleanPointable.class);
        try {
            tvp.getValue(bp);
            result[0] = Boolean.toString(bp.getBoolean());
            result[1] = epath + "/" + result[0];
        } finally {
            pp.giveBack(bp);
        }
        return result;
    }

    private String[] printByte(PrintStream ps, TaggedValuePointable tvp, String myid, String epath) {
        String[] result = { "", epath };
        BytePointable bp = pp.takeOne(BytePointable.class);
        try {
            tvp.getValue(bp);
            result[0] = Byte.toString(bp.byteValue());
            result[1] = epath + "/" + result[0];
        } finally {
            pp.giveBack(bp);
        }
        return result;
    }

    private String[] printDouble(PrintStream ps, TaggedValuePointable tvp, String myid, String epath) {
        String[] result = { "", epath };
        DoublePointable dp = pp.takeOne(DoublePointable.class);
        try {
            tvp.getValue(dp);
            abvs.reset();
            castToString.convertDouble(dp, dOut);
            result = printStringAbvs(ps, myid, epath);
        } catch (SystemException e) {
            e.printStackTrace();
        } catch (IOException e) {
            e.printStackTrace();
        } finally {
            pp.giveBack(dp);
        }
        return result;
    }

    private String[] printDate(PrintStream ps, TaggedValuePointable tvp, String myid, String epath) {
        String[] result = { "", epath };
        XSDatePointable dp = pp.takeOne(XSDatePointable.class);
        try {
            tvp.getValue(dp);
            abvs.reset();
            castToString.convertDate(dp, dOut);
            result = printStringAbvs(ps, myid, epath);
        } catch (SystemException e) {
            e.printStackTrace();
        } catch (IOException e) {
            e.printStackTrace();
        } finally {
            pp.giveBack(dp);
        }
        return result;
    }

    private String[] printDateTime(PrintStream ps, TaggedValuePointable tvp, String myid, String epath) {
        String[] result = { "", epath };
        XSDateTimePointable dtp = pp.takeOne(XSDateTimePointable.class);
        try {
            tvp.getValue(dtp);
            abvs.reset();
            castToString.convertDatetime(dtp, dOut);
            result = printStringAbvs(ps, myid, epath);
        } catch (SystemException e) {
            e.printStackTrace();
        } catch (IOException e) {
            e.printStackTrace();
        } finally {
            pp.giveBack(dtp);
        }
        return result;
    }

    private String[] printDTDuration(PrintStream ps, TaggedValuePointable tvp, String myid, String epath) {
        String[] result = { "", epath };
        LongPointable lp = pp.takeOne(LongPointable.class);
        try {
            tvp.getValue(lp);
            abvs.reset();
            castToString.convertDTDuration(lp, dOut);
            result = printStringAbvs(ps, myid, epath);
        } catch (SystemException e) {
            e.printStackTrace();
        } catch (IOException e) {
            e.printStackTrace();
        } finally {
            pp.giveBack(lp);
        }
        return result;
    }

    private String[] printDuration(PrintStream ps, TaggedValuePointable tvp, String myid, String epath) {
        String[] result = { "", epath };
        XSDurationPointable dp = pp.takeOne(XSDurationPointable.class);
        try {
            tvp.getValue(dp);
            abvs.reset();
            castToString.convertDuration(dp, dOut);
            result = printStringAbvs(ps, myid, epath);
        } catch (SystemException e) {
            e.printStackTrace();
        } catch (IOException e) {
            e.printStackTrace();
        } finally {
            pp.giveBack(dp);
        }
        return result;
    }

    private String[] printFloat(PrintStream ps, TaggedValuePointable tvp, String myid, String epath) {
        String[] result = { "", epath };
        FloatPointable fp = pp.takeOne(FloatPointable.class);
        try {
            tvp.getValue(fp);
            abvs.reset();
            castToString.convertFloat(fp, dOut);
            result = printStringAbvs(ps, myid, epath);
        } catch (SystemException e) {
            e.printStackTrace();
        } catch (IOException e) {
            e.printStackTrace();
        } finally {
            pp.giveBack(fp);
        }
        return result;
    }

    private String[] printGDay(PrintStream ps, TaggedValuePointable tvp, String myid, String epath) {
        String[] result = { "", epath };
        XSDatePointable dp = pp.takeOne(XSDatePointable.class);
        try {
            tvp.getValue(dp);
            abvs.reset();
            castToString.convertGDay(dp, dOut);
            result = printStringAbvs(ps, myid, epath);
        } catch (SystemException e) {
            e.printStackTrace();
        } catch (IOException e) {
            e.printStackTrace();
        } finally {
            pp.giveBack(dp);
        }
        return result;
    }

    private String[] printGMonth(PrintStream ps, TaggedValuePointable tvp, String myid, String epath) {
        String[] result = { "", epath };
        XSDatePointable dp = pp.takeOne(XSDatePointable.class);
        try {
            tvp.getValue(dp);
            abvs.reset();
            castToString.convertGMonth(dp, dOut);
            result = printStringAbvs(ps, myid, epath);
        } catch (SystemException e) {
            e.printStackTrace();
        } catch (IOException e) {
            e.printStackTrace();
        } finally {
            pp.giveBack(dp);
        }
        return result;
    }

    private String[] printGMonthDay(PrintStream ps, TaggedValuePointable tvp, String myid, String epath) {
        String[] result = { "", epath };
        XSDatePointable dp = pp.takeOne(XSDatePointable.class);
        try {
            tvp.getValue(dp);
            abvs.reset();
            castToString.convertGMonthDay(dp, dOut);
            result = printStringAbvs(ps, myid, epath);
        } catch (SystemException e) {
            e.printStackTrace();
        } catch (IOException e) {
            e.printStackTrace();
        } finally {
            pp.giveBack(dp);
        }
        return result;
    }

    private String[] printGYear(PrintStream ps, TaggedValuePointable tvp, String myid, String epath) {
        String[] result = { "", epath };
        XSDatePointable dp = pp.takeOne(XSDatePointable.class);
        try {
            tvp.getValue(dp);
            abvs.reset();
            castToString.convertGYear(dp, dOut);
            result = printStringAbvs(ps, myid, epath);
        } catch (SystemException e) {
            e.printStackTrace();
        } catch (IOException e) {
            e.printStackTrace();
        } finally {
            pp.giveBack(dp);
        }
        return result;
    }

    private String[] printGYearMonth(PrintStream ps, TaggedValuePointable tvp, String myid, String epath) {
        String[] result = { "", epath };
        XSDatePointable dp = pp.takeOne(XSDatePointable.class);
        try {
            tvp.getValue(dp);
            abvs.reset();
            castToString.convertGYearMonth(dp, dOut);
            result = printStringAbvs(ps, myid, epath);
        } catch (SystemException e) {
            e.printStackTrace();
        } catch (IOException e) {
            e.printStackTrace();
        } finally {
            pp.giveBack(dp);
        }
        return result;
    }

    private String[] printHexBinary(PrintStream ps, TaggedValuePointable tvp, String myid, String epath) {
        String[] result = { "", epath };
        XSBinaryPointable bp = pp.takeOne(XSBinaryPointable.class);
        try {
            tvp.getValue(bp);
            abvs.reset();
            castToString.convertHexBinary(bp, dOut);
            result = printStringAbvs(ps, myid, epath);
        } catch (SystemException e) {
            e.printStackTrace();
        } catch (IOException e) {
            e.printStackTrace();
        } finally {
            pp.giveBack(bp);
        }
        return result;
    }

    private String[] printInt(PrintStream ps, TaggedValuePointable tvp, String myid, String epath) {
        String[] result = { "", epath };
        IntegerPointable ip = pp.takeOne(IntegerPointable.class);
        try {
            tvp.getValue(ip);
            result[0] = Integer.toString(ip.intValue());
            result[1] = epath + "/" + result[0];
        } finally {
            pp.giveBack(ip);
        }
        return result;
    }

    private String[] printInteger(PrintStream ps, TaggedValuePointable tvp, String myid, String epath) {
        String[] result = { "", epath };
        LongPointable lp = pp.takeOne(LongPointable.class);
        try {
            tvp.getValue(lp);
            result[0] = Long.toString(lp.longValue());
            result[1] = epath + "/" + result[0];
        } finally {
            pp.giveBack(lp);
        }
        return result;
    }

    private String[] printShort(PrintStream ps, TaggedValuePointable tvp, String myid, String epath) {
        ShortPointable sp = pp.takeOne(ShortPointable.class);
        String[] result = { "", epath };
        try {
            tvp.getValue(sp);
            result[0] = Short.toString(sp.shortValue());
            result[1] = epath + "/" + result[0];
        } finally {
            pp.giveBack(sp);
        }
        return result;
    }

    private String[] printQName(PrintStream ps, TaggedValuePointable tvp, String myid, String epath) {
        XSQNamePointable dp = pp.takeOne(XSQNamePointable.class);
        String[] result = { "", epath };
        try {
            tvp.getValue(dp);
            abvs.reset();
            castToString.convertQName(dp, dOut);
            result = printStringAbvs(ps, myid, epath);
        } catch (SystemException e) {
            e.printStackTrace();
        } catch (IOException e) {
            e.printStackTrace();
        } finally {
            pp.giveBack(dp);
        }
        return result;
    }

    private String[] printStringAbvs(PrintStream ps, String myid, String epath) {
        UTF8StringPointable utf8sp = pp.takeOne(UTF8StringPointable.class);
        String[] result = { "", epath };
        try {
            utf8sp.set(abvs.getByteArray(), abvs.getStartOffset() + 1, abvs.getLength() - 1);
            result = printString(ps, utf8sp, myid, epath);
        } finally {
            pp.giveBack(utf8sp);
        }
        return result;
    }

    private String[] printString(PrintStream ps, TaggedValuePointable tvp, String myid, String epath) {
        UTF8StringPointable utf8sp = pp.takeOne(UTF8StringPointable.class);
        String[] result = { "", epath };
        try {
            tvp.getValue(utf8sp);
            result = printString(ps, utf8sp, myid, epath);
        } finally {
            pp.giveBack(utf8sp);
        }
        return result;
    }

    private String[] printString(PrintStream ps, UTF8StringPointable utf8sp, String myid, String epath) {
        int utfLen = utf8sp.getUTFLength();
        int offset = 2;
        String[] result = { "", epath };
        while (utfLen > 0) {
            char c = utf8sp.charAt(offset);
            switch (c) {
                case '<':
                    result[0] += "&lt;";
                    break;

                case '>':
                    result[0] += "&gt;";
                    break;

                case '&':
                    result[0] += "&amp;";
                    break;

                case '"':
                    result[0] += "&quot;";
                    break;

                case '\'':
                    result[0] += "&apos;";
                    break;

                default:
                    result[0] += c;
                    break;
            }
            int cLen = UTF8StringPointable.getModifiedUTF8Len(c);
            offset += cLen;
            utfLen -= cLen;

        }
        result[1] = epath + "/" + result[0];
        return result;
    }

    private String[] printTime(PrintStream ps, TaggedValuePointable tvp, String myid, String epath) {
        XSTimePointable tp = pp.takeOne(XSTimePointable.class);
        String[] result = { "", epath };
        try {
            tvp.getValue(tp);
            abvs.reset();
            castToString.convertTime(tp, dOut);
            result = printStringAbvs(ps, myid, epath);
        } catch (SystemException e) {
            e.printStackTrace();
        } catch (IOException e) {
            e.printStackTrace();
        } finally {
            pp.giveBack(tp);
        }
        return result;
    }

    private String[] printYMDuration(PrintStream ps, TaggedValuePointable tvp, String myid, String epath) {
        IntegerPointable ip = pp.takeOne(IntegerPointable.class);
        String[] result = { "", epath };
        try {
            tvp.getValue(ip);
            abvs.reset();
            castToString.convertYMDuration(ip, dOut);
            result = printStringAbvs(ps, myid, epath);
        } catch (SystemException e) {
            e.printStackTrace();
        } catch (IOException e) {
            e.printStackTrace();
        } finally {
            pp.giveBack(ip);
        }
        return result;
    }

}