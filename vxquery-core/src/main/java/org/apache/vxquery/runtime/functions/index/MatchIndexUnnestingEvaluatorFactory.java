package org.apache.vxquery.runtime.functions.index;

import java.io.DataInputStream;
import java.io.File;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Arrays;
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
import org.apache.lucene.queryparser.classic.ParseException;
import org.apache.lucene.queryparser.classic.QueryParser;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.ScoreDoc;
import org.apache.lucene.search.Sort;
import org.apache.lucene.search.SortField;
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

public class MatchIndexUnnestingEvaluatorFactory extends newAbstractTaggedValueArgumentUnnestingEvaluatorFactory {
    private static final long serialVersionUID = 1L;

    public MatchIndexUnnestingEvaluatorFactory(IScalarEvaluatorFactory[] args) {
        super(args);
    }

    @Override
    protected IUnnestingEvaluator createEvaluator(IHyracksTaskContext ctx, IScalarEvaluator[] args)
            throws AlgebricksException {

        return new AbstractTaggedValueArgumentUnnestingEvaluator(args) {

            private boolean first;
            private ArrayBackedValueStorage nodeAbvs;

            private int indexplace;
            private int indexlength;
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
            Sort resultsort;
            int fileslookedat = 0;
            int numindexlookups = 0;

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
                    indexplace = parse(nodeAbvs, indexplace);
                    indexplace = checkoverflow(indexplace);
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
                resultsort = new Sort(new SortField("path", SortField.Type.STRING),
                        new SortField("id", SortField.Type.STRING));
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
                    parser = new QueryParser(Version.LUCENE_40, "path", analyzer);

                    //Parser doesn't like / so paths are saved as name.name.
                    String betterelementpath = elementpath.replaceAll("/", ".");

                    //TODO: NEED TO EXCLUDE OTHER THINGS HERE> THIS WOULD FIND /bookdoodle/* when we look for /book
                    String special = "epath:" + betterelementpath + "*";

                    TopDocs results = null;
                    try {
                        query = parser.parse(special);
                        try {
                            //Get the first 1 million lines from the index
                            results = searcher.search(query, 1000000, resultsort);
                        } catch (IOException e) {
                            e.printStackTrace();
                        }
                    } catch (ParseException e) {
                        e.printStackTrace();
                    }

                    hits = results.scoreDocs;
                    indexplace = 0;
                    indexlength = hits.length;
                }
            }

            /*This function creates an abvsFileNode, continuing until it
             * reaches a new filename
             */
            public int parse(ArrayBackedValueStorage abvsFileNode, int indexplace) {

                if (hits.length > 0) {
                    try {
                        try {
                            handler.startDocument();
                            String thispath = searcher.doc(hits[indexplace].doc).get("path");
                            int returner = buildelement(abvsFileNode, indexplace);

                            returner = checkoverflow(returner);
                            while (returner + 1 < hits.length) {
                                if (searcher.doc(hits[returner + 1].doc).get("path").equals(thispath)) {
                                    returner = buildelement(abvsFileNode, returner + 1);
                                    returner = checkoverflow(returner);
                                } else {
                                    break;
                                }
                            }

                            handler.endDocument();
                            handler.write(abvsFileNode);
                            return returner;
                        } catch (IOException e) {
                            // TODO Auto-generated catch block
                            e.printStackTrace();
                        }
                    } catch (SAXException e) {
                        // TODO Auto-generated catch block
                        e.printStackTrace();
                    }
                }
                return hits.length;
            }

            /*This is the recursive element node builder
             * 
             */
            private int buildelement(ArrayBackedValueStorage abvsFileNode, int indexplace) {
                int whereifinish = indexplace;
                Document doc = null;
                try {
                    doc = searcher.doc(hits[indexplace].doc);
                } catch (IOException e1) {
                    // TODO Auto-generated catch block
                    e1.printStackTrace();
                }
                String uri = "";
                String contents = doc.get("contents");
                String type = doc.get("type");
                if (type.equals("textnode")) {
                    char[] charcontents = contents.toCharArray();
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
                    whereifinish = checkoverflow(whereifinish);
                    whereifinish = findattributechildren(doc, whereifinish, names, values, uris, localnames, types,
                            qnames);
                    Attributes atts = new indexattributes(names, values, uris, localnames, types, qnames);
                    try {

                        handler.startElement(uri, contents, contents, atts);
                        try {
                            boolean nomorechildren = false;
                            whereifinish = checkoverflow(whereifinish);

                            while (whereifinish + 1 < hits.length && !nomorechildren) {
                                if (ischild(searcher.doc(hits[whereifinish + 1].doc), doc)) {
                                    whereifinish = buildelement(abvsFileNode, whereifinish + 1);
                                    whereifinish = checkoverflow(whereifinish);
                                } else {
                                    nomorechildren = true;
                                }
                            }
                        } catch (IOException e) {
                            e.printStackTrace();
                        }
                        handler.endElement(uri, contents, contents);
                    } catch (SAXException e) {
                        e.printStackTrace();
                    }

                }
                return whereifinish;
            }

            /*This function creates the attribute children for an element node
             * 
             */
            int findattributechildren(Document doc, int indexplace, Vector<String> n, Vector<String> v,
                    Vector<String> u, Vector<String> l, Vector<String> t, Vector<String> q) {
                indexplace = checkoverflow(indexplace);
                int nextindex = indexplace + 1;
                boolean foundattributes = false;
                if (nextindex < hits.length) {
                    Document nextguy;
                    try {
                        nextguy = searcher.doc(hits[nextindex].doc);
                        while (nextindex < hits.length && ischild(nextguy, doc)
                                && nextguy.get("type").equals("attribute")) {
                            if (isdirectchildattribute(nextguy, doc)) {
                                foundattributes = true;
                                n.add(nextguy.get("contents"));
                                nextindex = checkoverflow(nextindex);
                                v.add(searcher.doc(hits[nextindex + 1].doc).get("contents"));
                                u.add(nextguy.get("contents"));
                                l.add(nextguy.get("contents"));
                                t.add(nextguy.get("contents"));
                                q.add(nextguy.get("contents"));
                            }
                            nextindex += 1;
                            nextindex = checkoverflow(nextindex);
                            if (nextindex == -1) {
                                nextindex = 0;
                                nextguy = searcher.doc(hits[nextindex].doc);
                            } else {
                                nextindex += 1;
                                nextguy = searcher.doc(hits[nextindex].doc);
                            }

                        }
                    } catch (IOException e) {
                        // TODO Auto-generated catch block
                        e.printStackTrace();
                    }
                }
                if (foundattributes) {
                    return nextindex - 1;

                } else {
                    return indexplace;
                }
            }

            boolean ischild(Document child, Document adult) {
                String childid = child.get("id");
                String adultid = adult.get("id");
                String childdoc = child.get("path");
                String adultdoc = adult.get("path");
                if (childid.startsWith(adultid + ".") && (childdoc.equals(adultdoc))) {
                    //System.out.println("bookstore had a child!\n");
                    return true;
                }
                return false;
            }

            boolean isdirectchildattribute(Document child, Document adult) {
                String childid = child.get("id");
                String adultid = adult.get("id");
                String childtype = child.get("type");
                int numdotschild = childid.split("\\.").length - 1;
                int numdotsadult = adultid.split("\\.").length - 1;
                if (childid.startsWith(adultid + ".") && (numdotschild == (numdotsadult + 1))
                        && childtype.equals("attribute")) {
                    return true;
                }
                return false;
            }

            /*This function checks for overflow. Once we have looked at a batch of 1 million
             * hits, we start on the next batch
             */
            int checkoverflow(int currentplace) {
                if (currentplace + 1 >= hits.length) {
                    try {
                        ScoreDoc[] newhits = searcher.searchAfter(hits[hits.length - 1], query, null, 1000000,
                                resultsort).scoreDocs;
                        if (newhits.length > 0) {
                            hits = newhits;
                            indexlength = hits.length;
                            numindexlookups += 1;
                            return -1;
                        }
                    } catch (IOException e) {
                        e.printStackTrace();
                    }
                }
                return currentplace;
            }

        };
    }
}