/*
* Licensed to the Apache Software Foundation (ASF) under one or more
* contributor license agreements.  See the NOTICE file distributed with
* this work for additional information regarding copyright ownership.
* The ASF licenses this file to You under the Apache License, Version 2.0
* (the "License"); you may not use this file except in compliance with
* the License.  You may obtain a copy of the License at
*
*     http://www.apache.org/licenses/LICENSE-2.0
*
* Unless required by applicable law or agreed to in writing, software
* distributed under the License is distributed on an "AS IS" BASIS,
* WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
* See the License for the specific language governing permissions and
* limitations under the License.
*/
package org.apache.vxquery.runtime.functions.index;

import java.io.IOException;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.List;

import org.apache.hyracks.algebricks.common.exceptions.AlgebricksException;
import org.apache.hyracks.api.comm.IFrameFieldAppender;
import org.apache.hyracks.api.comm.IFrameWriter;
import org.apache.hyracks.api.context.IHyracksTaskContext;
import org.apache.hyracks.data.std.api.IPointable;
import org.apache.hyracks.data.std.util.ArrayBackedValueStorage;
import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.document.Document;
import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.IndexableField;
import org.apache.lucene.queryparser.classic.QueryParser;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.ScoreDoc;
import org.apache.lucene.search.TopDocs;
import org.apache.lucene.store.FSDirectory;
import org.apache.vxquery.context.DynamicContext;
import org.apache.vxquery.exceptions.ErrorCode;
import org.apache.vxquery.exceptions.SystemException;
import org.apache.vxquery.index.IndexAttributes;
import org.apache.vxquery.runtime.functions.util.FunctionHelper;
import org.apache.vxquery.types.ElementType;
import org.apache.vxquery.types.NameTest;
import org.apache.vxquery.types.NodeType;
import org.apache.vxquery.types.SequenceType;
import org.apache.vxquery.xmlparser.ITreeNodeIdProvider;
import org.apache.vxquery.xmlparser.SAXContentHandler;
import org.apache.vxquery.xmlparser.TreeNodeIdProvider;
import org.xml.sax.Attributes;
import org.xml.sax.SAXException;

public class VXQueryIndexReader {

    private ArrayBackedValueStorage nodeAbvs = new ArrayBackedValueStorage();

    private int indexPlace;
    private int indexLength;
    private String elementPath;
    private String indexName;
    private List<SequenceType> childSequenceTypes;
    private IndexReader reader;
    private IndexSearcher searcher;
    private QueryParser parser;
    private ScoreDoc[] hits;
    private SAXContentHandler handler;
    private Query query;
    private Document doc;
    private List<IndexableField> fields;
    private IHyracksTaskContext ctx;
    private String[] childLocalName = null;
    private IFrameFieldAppender appender;
    private boolean firstElement;

    public VXQueryIndexReader(IHyracksTaskContext context, String indexPath, List<Integer> childSeq,
            IFrameFieldAppender appender) {
        this.ctx = context;
        this.indexName = indexPath;
        this.appender = appender;
        final DynamicContext dCtx = (DynamicContext) ctx.getJobletContext().getGlobalJobData();
        childSequenceTypes = new ArrayList<>();
        for (int typeCode : childSeq) {
            childSequenceTypes.add(dCtx.getStaticContext().lookupSequenceType(typeCode));
        }
        childLocalName = new String[childSequenceTypes.size()];
        int index = 0;
        StringBuilder stb = new StringBuilder();
        stb.append("/");
        for (SequenceType sType : childSequenceTypes) {
            NodeType nodeType = (NodeType) sType.getItemType();
            ElementType eType = (ElementType) nodeType;
            NameTest nameTest = eType.getNameTest();
            childLocalName[index] = FunctionHelper.getStringFromBytes(nameTest.getLocalName());

            stb.append(childLocalName[index]);
            if (index != childSequenceTypes.size() - 1) {
                stb.append("/");
            }
            ++index;
        }
        elementPath = stb.toString();
    }

    public boolean step(IPointable result, IFrameWriter writer, int tupleIndex) throws AlgebricksException {
        /*each step will create a tuple for a single xml file
        * This is done using the parse function
        * checkoverflow is used throughout. This is because memory might not be
        * able to hold all of the results at once, so we return 1 million at
        * a time and check when we need to get more
        */
        if (indexPlace < indexLength) {
            nodeAbvs.reset();
            try {
                //TODO: now we get back the entire document
                doc = searcher.doc(hits[indexPlace].doc);
                fields = doc.getFields();
                handler.setupElementWriter(writer, tupleIndex);
                this.firstElement = true;
                parse(nodeAbvs);
            } catch (IOException e) {
                throw new AlgebricksException(e);
            }
            indexPlace += 1;
            result.set(nodeAbvs.getByteArray(), nodeAbvs.getStartOffset(), nodeAbvs.getLength());
            return true;
        }
        return false;
    }

    public void init() throws SystemException {

        int partition = ctx.getTaskAttemptId().getTaskId().getPartition();
        ITreeNodeIdProvider nodeIdProvider = new TreeNodeIdProvider((short) partition);
        handler = new SAXContentHandler(false, nodeIdProvider, appender, childSequenceTypes);

        nodeAbvs.reset();
        indexPlace = 0;

        try {
            indexPlace = 0;

            //Create the index reader.
            reader = DirectoryReader.open(FSDirectory.open(Paths.get(indexName)));
        } catch (IOException e) {
            throw new SystemException(ErrorCode.SYSE0001, e);
        }

        searcher = new IndexSearcher(reader);
        Analyzer analyzer = new CaseSensitiveAnalyzer();

        parser = new CaseSensitiveQueryParser("item", analyzer);

        String queryString = elementPath.replaceAll("/", ".");
        queryString = "item:" + queryString + "*";

        int lastslash = elementPath.lastIndexOf('/');
        elementPath = elementPath.substring(0, lastslash) + ":" + elementPath.substring(lastslash + 1);
        elementPath = elementPath.replaceAll("/", ".") + ".element";

        TopDocs results = null;
        try {
            query = parser.parse(queryString);

            //TODO: Right now it only returns 1000000 results
            results = searcher.search(query, 1000000);
        } catch (Exception e) {
            throw new SystemException(null, e);
        }

        hits = results.scoreDocs;
        indexPlace = 0;
        indexLength = hits.length;
    }

    public void parse(ArrayBackedValueStorage abvsFileNode) throws IOException {
        try {
            for (int i = 0; i < fields.size(); i++) {
                String fieldValue = fields.get(i).stringValue();
                if (fieldValue.equals(elementPath)) {
                    handler.startDocument();
                    this.firstElement = true;
                    buildElement(abvsFileNode, i);
                }
            }
        } catch (Exception e) {
            throw new IOException(e);
        }
    }

    private int buildElement(ArrayBackedValueStorage abvsFileNode, int fieldNum) throws SAXException {
        int whereIFinish = fieldNum;
        int firstFinish;
        IndexableField field = fields.get(fieldNum);
        String contents = field.stringValue();
        String uri = "";

        int firstColon = contents.indexOf(':');
        int lastDot = contents.lastIndexOf('.');
        String type = contents.substring(lastDot + 1);
        String lastBit = contents.substring(firstColon + 1, lastDot);

        if (this.firstElement) {
            this.firstElement = false;
            firstFinish = whereIFinish - this.childSequenceTypes.size() + 1;
            String firstBit = contents.substring(1, firstColon);
            List<String> names = new ArrayList<>();
            List<String> values = new ArrayList<>();
            List<String> uris = new ArrayList<>();
            List<String> localNames = new ArrayList<>();
            List<String> types = new ArrayList<>();
            List<String> qNames = new ArrayList<>();
            firstFinish = findAttributeChildren(firstFinish, names, values, uris, localNames, types, qNames);
            Attributes atts = new IndexAttributes(names, values, uris, localNames, types, qNames);

            handler.startElement(uri, firstBit, firstBit, atts);
            buildElement(abvsFileNode, firstFinish + 1);
            handler.endElement(uri, firstBit, firstBit);

        }

        if ("textnode".equals(type)) {
            char[] charContents = lastBit.toCharArray();
            handler.characters(charContents, 0, charContents.length);

        }
        if ("element".equals(type)) {
            List<String> names = new ArrayList<>();
            List<String> values = new ArrayList<>();
            List<String> uris = new ArrayList<>();
            List<String> localNames = new ArrayList<>();
            List<String> types = new ArrayList<>();
            List<String> qNames = new ArrayList<>();
            whereIFinish = findAttributeChildren(whereIFinish, names, values, uris, localNames, types, qNames);
            Attributes atts = new IndexAttributes(names, values, uris, localNames, types, qNames);

            handler.startElement(uri, lastBit, lastBit, atts);

            boolean noMoreChildren = false;

            while (whereIFinish + 1 < fields.size() && !noMoreChildren) {
                if (isChild(fields.get(whereIFinish + 1), field)) {
                    whereIFinish = buildElement(abvsFileNode, whereIFinish + 1);
                } else {
                    noMoreChildren = true;
                }
            }

            handler.endElement(uri, lastBit, lastBit);

        }
        return whereIFinish;
    }

    /*This function creates the attribute children for an element node
     *
     */
    int findAttributeChildren(int fieldnum, List<String> n, List<String> v, List<String> u, List<String> l,
            List<String> t, List<String> q) {
        int nextindex = fieldnum + 1;
        boolean foundattributes = false;
        if (nextindex < fields.size()) {
            IndexableField nextguy;

            while (nextindex < fields.size()) {
                nextguy = fields.get(nextindex);
                String contents = nextguy.stringValue();
                int firstcolon = contents.indexOf(':');
                int lastdot = contents.lastIndexOf('.');
                String lastbit = contents.substring(firstcolon + 1, lastdot);

                if (isDirectChildAttribute(nextguy, fields.get(fieldnum))) {
                    foundattributes = true;
                    n.add(lastbit);
                    IndexableField nextnextguy = fields.get(nextindex + 1);
                    contents = nextnextguy.stringValue();
                    firstcolon = contents.indexOf(':');
                    lastdot = contents.lastIndexOf('.');
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

    boolean isChild(IndexableField child, IndexableField adult) {
        String childId = child.stringValue();
        String adultId = adult.stringValue();

        int lastDotChild = childId.lastIndexOf('.');
        int lastDotAdult = adultId.lastIndexOf('.');

        String childPath = childId.substring(0, lastDotChild);
        String adultPath = adultId.substring(0, lastDotAdult);
        adultPath = adultPath.replaceFirst(":", ".");

        return childPath.startsWith(adultPath + ":") || childPath.startsWith(adultPath + ".");
    }

    boolean isDirectChildAttribute(IndexableField child, IndexableField adult) {
        String childId = child.stringValue();
        String adultId = adult.stringValue();

        String childPath = childId.substring(0, childId.lastIndexOf('.'));
        String adultPath = adultId.substring(0, adultId.lastIndexOf('.'));
        adultPath = adultPath.replaceFirst(":", ".");
        String[] childSegments = child.stringValue().split("\\.");

        String childType = childSegments[childSegments.length - 1];

        return childPath.startsWith(adultPath + ":") && "attribute".equals(childType);
    }

}
