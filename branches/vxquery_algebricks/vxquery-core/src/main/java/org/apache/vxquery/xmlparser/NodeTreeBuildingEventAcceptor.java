/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 * http://www.apache.org/licenses/LICENSE-2.0
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.vxquery.xmlparser;

import java.io.DataOutput;
import java.util.ArrayList;

import org.apache.vxquery.datamodel.builders.DictionaryBuilder;
import org.apache.vxquery.datamodel.builders.ElementNodeBuilder;
import org.apache.vxquery.datamodel.builders.base.IDMBuilderPool;
import org.apache.vxquery.exceptions.SystemException;
import org.apache.vxquery.types.BuiltinTypeQNames;
import org.apache.vxquery.v0datamodel.XDMItem;
import org.apache.vxquery.xmlquery.query.XQueryConstants;

public class NodeTreeBuildingEventAcceptor implements IEventAcceptor {
    private final IDMBuilderPool dmBuilderPool;

    private final DataOutput out;

    private final boolean createNodeIds;

    private final boolean attachTypes;

    private DictionaryBuilder db;

    private ArrayList<ElementNodeBuilder> enbStack;

    public NodeTreeBuildingEventAcceptor(IDMBuilderPool dmBuilderPool, DataOutput out, boolean createNodeIds,
            boolean attachTypes, ITreeNodeIdProvider idProvider) {
        this.dmBuilderPool = dmBuilderPool;
        this.out = out;
        this.createNodeIds = createNodeIds;
        this.attachTypes = attachTypes;
    }

    @Override
    public void open() throws SystemException {
    }

    @Override
    public void startDocument() throws SystemException {
        db = dmBuilderPool.getDictionaryBuilder();
    }

    @Override
    public void endDocument() throws SystemException {
        dmBuilderPool.returnDictionaryBuilder(db);
    }

    @Override
    public void startElement(String uri, String localName, String prefix) throws SystemException {
        ElementNodeBuilder enb = dmBuilderPool.getElementNodeBuilder();
        enb.reset();
        int uriCode = db.lookup(uri);
        int localNameCode = db.lookup(localName);
        int prefixCode = db.lookup(prefix);
        enb.setName(uriCode, localNameCode, prefixCode);
        if (attachTypes) {
            int typeUriCode = db.lookup(XQueryConstants.XS_NSURI);
            int typeLocalNameCode = db.lookup(BuiltinTypeQNames.UNTYPED_STR);
            int typePrefixCode = db.lookup(XQueryConstants.XS_PREFIX);
            enb.setType(typeUriCode, typeLocalNameCode, typePrefixCode);
        }
        enbStack.add(enb);
    }

    @Override
    public void endElement() throws SystemException {
        ElementNodeBuilder enb = enbStack.remove(enbStack.size() - 1);

    }

    @Override
    public void namespace(String prefix, String uri) throws SystemException {

    }

    @Override
    public void attribute(String uri, String localName, String prefix, String stringValue) throws SystemException {

    }

    @Override
    public void text(char[] chars, int start, int length) throws SystemException {

    }

    @Override
    public void comment(String content) throws SystemException {

    }

    @Override
    public void pi(String target, String content) throws SystemException {

    }

    @Override
    public void item(XDMItem item) throws SystemException {

    }

    @Override
    public void close() {

    }
}