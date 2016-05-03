package org.apache.vxquery.runtime.functions.index;

import java.io.DataInputStream;
import java.io.File;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.List;
import java.util.Vector;

import org.apache.hyracks.algebricks.common.exceptions.AlgebricksException;
import org.apache.hyracks.algebricks.runtime.base.IScalarEvaluator;
import org.apache.hyracks.algebricks.runtime.base.IScalarEvaluatorFactory;
import org.apache.hyracks.algebricks.runtime.base.IUnnestingEvaluator;
import org.apache.hyracks.api.context.IHyracksTaskContext;
import org.apache.hyracks.data.std.api.IPointable;
import org.apache.hyracks.data.std.primitive.UTF8StringPointable;
import org.apache.hyracks.data.std.util.ArrayBackedValueStorage;
import org.apache.hyracks.dataflow.common.comm.util.ByteBufferInputStream;
import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.analysis.standard.StandardAnalyzer;
import org.apache.lucene.document.Document;
import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.IndexableField;
import org.apache.lucene.queryparser.classic.ParseException;
import org.apache.lucene.queryparser.classic.QueryParser;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.ScoreDoc;
import org.apache.lucene.search.TopDocs;
import org.apache.lucene.store.FSDirectory;
import org.apache.lucene.util.Version;
import org.apache.vxquery.datamodel.accessors.TaggedValuePointable;
import org.apache.vxquery.datamodel.values.ValueTag;
import org.apache.vxquery.exceptions.ErrorCode;
import org.apache.vxquery.exceptions.SystemException;
import org.apache.vxquery.index.SAXIndexHandler;
import org.apache.vxquery.index.indexattributes;
import org.apache.vxquery.runtime.functions.base.AbstractTaggedValueArgumentUnnestingEvaluator;
import org.apache.vxquery.runtime.functions.base.newAbstractTaggedValueArgumentUnnestingEvaluatorFactory;
import org.apache.vxquery.xmlparser.ITreeNodeIdProvider;
import org.apache.vxquery.xmlparser.TreeNodeIdProvider;
import org.xml.sax.Attributes;
import org.xml.sax.SAXException;

public class MatchIndexDocUnnestingEvaluatorFactory extends newAbstractTaggedValueArgumentUnnestingEvaluatorFactory {
    private static final long serialVersionUID = 1L;

    public MatchIndexDocUnnestingEvaluatorFactory(IScalarEvaluatorFactory[] args) {
        super(args);
    }

    @Override
    protected IUnnestingEvaluator createEvaluator(IHyracksTaskContext ctx, IScalarEvaluator[] args)
            throws AlgebricksException {

        return new AbstractTaggedValueArgumentUnnestingEvaluator(args) {

            private boolean first;
            private boolean matchin;
            private ArrayBackedValueStorage nodeAbvs;

            private int indexplace;
            private int indexlength;
            private int currentelement;
            private String elementpath;
            private String IndexName;
            private String match;

            private UTF8StringPointable stringindexfolder = (UTF8StringPointable) UTF8StringPointable.FACTORY
                    .createPointable();
            private UTF8StringPointable stringelementpath = (UTF8StringPointable) UTF8StringPointable.FACTORY
                    .createPointable();
            private UTF8StringPointable stringmatch = (UTF8StringPointable) UTF8StringPointable.FACTORY
                    .createPointable();
            private ByteBufferInputStream bbis = new ByteBufferInputStream();
            private DataInputStream di = new DataInputStream(bbis);

            private IndexReader reader;
            private IndexSearcher searcher;
            private Analyzer analyzer;
            private QueryParser parser;
            ScoreDoc[] hits;
            SAXIndexHandler handler;
            Query query;
            int fileslookedat = 0;
            int numindexlookups = 0;
            Document doc;
            List<IndexableField> fields;

            @Override
            public boolean step(IPointable result) throws AlgebricksException {
                /* each step will create a tuple for a single xml file
                 * This is done using the parse function
                 * checkoverflow is used throughout. This is because memory might not be
                 * able to hold all of the results at once, so we return 1 million at
                 * a time and check when we need to get more
                 */
                if (indexplace < indexlength) {
                    int partition = ctxview.getTaskAttemptId().getTaskId().getPartition();
                    ITreeNodeIdProvider nodeIdProvider = new TreeNodeIdProvider((short) partition);
                    handler = new SAXIndexHandler(false, nodeIdProvider);
                    nodeAbvs.reset();
                    try {
                        doc = searcher.doc(hits[indexplace].doc);
                        fields = doc.getFields();
                        parse(nodeAbvs);
                    } catch (IOException e) {
                        // TODO Auto-generated catch block
                        e.printStackTrace();
                    }
                    indexplace += 1;
                    result.set(nodeAbvs.getByteArray(), nodeAbvs.getStartOffset(), nodeAbvs.getLength());
                    fileslookedat += 1;
                    return true;
                }
                System.out.println("looked at: " + fileslookedat);
                System.out.println("Used index this many times: " + numindexlookups);
                return false;
            }

            @Override
            protected void init(TaggedValuePointable[] args) throws SystemException {
                first = true;
                if (first) {
                    nodeAbvs = new ArrayBackedValueStorage();
                    indexplace = 0;
                    TaggedValuePointable tvp1 = args[0];
                    TaggedValuePointable tvp2 = args[1];
                    TaggedValuePointable tvp3 = args[2];

                    // TODO add support empty sequence and no argument.
                    if (tvp1.getTag() != ValueTag.XS_STRING_TAG || tvp2.getTag() != ValueTag.XS_STRING_TAG
                            || tvp3.getTag() != ValueTag.XS_STRING_TAG) {
                        throw new SystemException(ErrorCode.FORG0006);
                    }
                    tvp1.getValue(stringindexfolder);
                    tvp2.getValue(stringelementpath);
                    tvp3.getValue(stringmatch);
                    //This whole loop is to get the string arguments, indefolder, elementpath, and match option
                    try {
                        // Get the list of files.
                        bbis.setByteBuffer(ByteBuffer.wrap(
                                Arrays.copyOfRange(stringindexfolder.getByteArray(), stringindexfolder.getStartOffset(),
                                        stringindexfolder.getLength() + stringindexfolder.getStartOffset())),
                                0);
                        IndexName = di.readUTF();
                        bbis.setByteBuffer(ByteBuffer.wrap(
                                Arrays.copyOfRange(stringelementpath.getByteArray(), stringelementpath.getStartOffset(),
                                        stringelementpath.getLength() + stringelementpath.getStartOffset())),
                                0);
                        elementpath = di.readUTF();
                        bbis.setByteBuffer(ByteBuffer.wrap(Arrays.copyOfRange(stringmatch.getByteArray(),
                                stringmatch.getStartOffset(), stringmatch.getLength() + stringmatch.getStartOffset())),
                                0);
                        match = di.readUTF();
                    } catch (IOException e) {
                        throw new SystemException(ErrorCode.SYSE0001, e);
                    }
                    indexplace = 0;
                    first = false;
                    reader = null;
                    //Create the index reader.
                    try {
                        reader = DirectoryReader.open(FSDirectory.open(new File(IndexName)));
                    } catch (IOException e1) {
                        e1.printStackTrace();
                    }
                    searcher = new IndexSearcher(reader);
                    analyzer = new StandardAnalyzer(Version.LUCENE_40);

                    parser = new QueryParser(Version.LUCENE_40, "item", analyzer);

                    //Parser doesn't like / so paths are saved as name.name....name:elementname.element
                    //Can NOT search with spaces
                    matchin = true;
                    if (match.equals("")) {
                        matchin = false;
                    }
                    String parsematch = match.replaceFirst("=", "?");
                    String parseelementpath = elementpath.replaceAll("/", ".");
                    parsematch = parseelementpath + "." + parsematch + ".textnode";

                    int lastslash = elementpath.lastIndexOf("/");
                    elementpath = elementpath.substring(0, lastslash) + ":" + elementpath.substring(lastslash + 1);
                    elementpath = elementpath.replaceAll("/", ".") + ".element";

                    match = parsematch.replaceFirst("\\?", ":");

                    if (elementpath.startsWith(":")) {
                        parseelementpath = elementpath.replaceFirst(".", "");
                        parseelementpath = "\\:" + parseelementpath + "*";
                    } else {
                        parseelementpath = elementpath.replaceFirst(":", "?");
                    }

                    TopDocs results = null;
                    try {
                        query = parser.parse(parseelementpath);
                        if (matchin) {
                            query = parser.parse(parsematch);
                        }
                        System.out.println(query.toString());
                        try {

                            results = searcher.search(query, 1000000);
                        } catch (IOException e) {
                            e.printStackTrace();
                        }
                    } catch (ParseException e) {
                        e.printStackTrace();
                    }

                    hits = results.scoreDocs;
                    System.out.println("found: " + results.totalHits);
                    indexplace = 0;
                    indexlength = hits.length;
                }
            }

            public void parse(ArrayBackedValueStorage abvsFileNode) {

                try {
                    handler.startDocument();
                    for (int i = 0; i < fields.size(); i++) {
                        if (fields.get(i).stringValue().equals(elementpath)) {
                            if ((i == fields.size() - 1) || !matchin) {
                                buildelement(abvsFileNode, i);
                            } else {
                                boolean foundmatch = false;
                                for (int j = i + 1; j < fields.size(); j++) {
                                    if (!ischild(fields.get(j), fields.get(i))) {
                                        break;
                                    }
                                    if (fields.get(j).stringValue().equals(match)) {
                                        foundmatch = true;
                                        break;
                                    }
                                }
                                if (foundmatch) {
                                    buildelement(abvsFileNode, i);
                                }
                            }
                        }
                    }

                    handler.endDocument();
                    handler.write(abvsFileNode);
                } catch (SAXException e) {
                    // TODO Auto-generated catch block
                    e.printStackTrace();
                } catch (IOException e) {
                    // TODO Auto-generated catch block
                    e.printStackTrace();
                }
            }

            private int buildelement(ArrayBackedValueStorage abvsFileNode, int fieldnum) {
                int whereifinish = fieldnum;
                IndexableField field = fields.get(fieldnum);
                String contents = field.stringValue();
                String uri = "";

                int firstcolon = contents.indexOf(":");
                int lastdot = contents.lastIndexOf(".");
                String type = contents.substring(lastdot + 1);
                String lastbit = contents.substring(firstcolon + 1, lastdot);

                if (type.equals("textnode")) {
                    char[] charcontents = lastbit.toCharArray();
                    try {
                        handler.characters(charcontents, 0, charcontents.length);
                    } catch (SAXException e) {
                        // TODO Auto-generated catch block
                        e.printStackTrace();
                    }
                }
                if (type.equals("element")) {
                    Vector<String> names = new Vector<String>();
                    Vector<String> values = new Vector<String>();
                    Vector<String> uris = new Vector<String>();
                    Vector<String> localnames = new Vector<String>();
                    Vector<String> types = new Vector<String>();
                    Vector<String> qnames = new Vector<String>();
                    whereifinish = findattributechildren(whereifinish, names, values, uris, localnames, types, qnames);
                    Attributes atts = new indexattributes(names, values, uris, localnames, types, qnames);
                    try {

                        handler.startElement(uri, lastbit, lastbit, atts);

                        boolean nomorechildren = false;

                        while (whereifinish + 1 < fields.size() && !nomorechildren) {
                            if (ischild(fields.get(whereifinish + 1), field)) {
                                whereifinish = buildelement(abvsFileNode, whereifinish + 1);
                            } else {
                                nomorechildren = true;
                            }
                        }

                        handler.endElement(uri, lastbit, lastbit);
                    } catch (SAXException e) {
                        e.printStackTrace();
                    }

                }
                return whereifinish;
            }

            /*This function creates the attribute children for an element node
             * 
             */
            int findattributechildren(int fieldnum, Vector<String> n, Vector<String> v, Vector<String> u,
                    Vector<String> l, Vector<String> t, Vector<String> q) {
                int nextindex = fieldnum + 1;
                boolean foundattributes = false;
                if (nextindex < fields.size()) {
                    IndexableField nextguy;

                    while (nextindex < fields.size()) {
                        nextguy = fields.get(nextindex);
                        String contents = nextguy.stringValue();
                        int firstcolon = contents.indexOf(":");
                        int lastdot = contents.lastIndexOf(".");
                        String lastbit = contents.substring(firstcolon + 1, lastdot);

                        if (isdirectchildattribute(nextguy, fields.get(fieldnum))) {
                            foundattributes = true;
                            n.add(lastbit);
                            IndexableField nextnextguy = fields.get(nextindex + 1);
                            contents = nextnextguy.stringValue();
                            firstcolon = contents.indexOf(":");
                            lastdot = contents.lastIndexOf(".");
                            String nextlastbit = contents.substring(firstcolon + 1, lastdot);
                            v.add(nextlastbit);
                            u.add(lastbit);
                            l.add(lastbit);
                            t.add(lastbit);
                            q.add(lastbit);
                        } else {
                            break;
                        }
                        nextindex += 2;
                    }
                }
                if (foundattributes) {
                    return nextindex - 1;

                } else {
                    return fieldnum;
                }
            }

            boolean ischild(IndexableField child, IndexableField adult) {
                String childid = child.stringValue();
                String adultid = adult.stringValue();

                int lastdotchild = childid.lastIndexOf(".");
                int lastdotadult = adultid.lastIndexOf(".");

                String childpath = childid.substring(0, lastdotchild);
                String adultpath = adultid.substring(0, lastdotadult);
                adultpath = adultpath.replaceFirst(":", ".");

                if (childpath.startsWith(adultpath + ":") || childpath.startsWith(adultpath + ".")) {
                    return true;
                }
                return false;
            }

            boolean isdirectchildattribute(IndexableField child, IndexableField adult) {
                String childid = child.stringValue();
                String adultid = adult.stringValue();

                String childpath = childid.substring(0, childid.lastIndexOf("."));
                String adultpath = adultid.substring(0, adultid.lastIndexOf("."));
                adultpath = adultpath.replaceFirst(":", ".");
                String[] childpieces = child.stringValue().split("\\.");

                String childtype = childpieces[childpieces.length - 1];

                if (childpath.startsWith(adultpath + ":") && childtype.equals("attribute")) {
                    return true;
                }
                return false;
            }

        };
    }
}