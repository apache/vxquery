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
package org.apache.vxquery.datamodel.dtm;

import org.apache.vxquery.datamodel.DatamodelHelper;
import org.apache.vxquery.datamodel.NameCache;
import org.apache.vxquery.datamodel.NodeConstructingEventAcceptor;
import org.apache.vxquery.datamodel.NodeFactory;
import org.apache.vxquery.datamodel.XDMItem;
import org.apache.vxquery.datamodel.XDMNode;
import org.apache.vxquery.datamodel.XDMSequence;
import org.apache.vxquery.datamodel.XDMValue;
import org.apache.vxquery.datamodel.atomic.AtomicValueFactory;
import org.apache.vxquery.datamodel.atomic.QNameValue;
import org.apache.vxquery.datamodel.atomic.StringValue;
import org.apache.vxquery.exceptions.SystemException;
import org.apache.vxquery.runtime.base.CloseableIterator;

public final class DTMNodeFactory implements NodeFactory {
    private NameCache nameCache;
    private AtomicValueFactory avf;

    public DTMNodeFactory(NameCache nameCache, AtomicValueFactory avf) {
        this.nameCache = nameCache;
        this.avf = avf;
    }

    @Override
    public XDMNode createAttribute(QNameValue name, QNameValue typeAnnotation, CloseableIterator content)
            throws SystemException {
        DTMBuildingEventAcceptor ea = new DTMBuildingEventAcceptor(nameCache, avf);
        ea.open();
        CharSequence strValue = DatamodelHelper.serialize(content);
        ea.attribute(name.getNameCache(), name.getCode(), strValue);
        ea.close();
        return ea.getConstructedNode();
    }

    @Override
    public XDMNode createComment(StringValue content) throws SystemException {
        DTMBuildingEventAcceptor ea = new DTMBuildingEventAcceptor(nameCache, avf);
        ea.open();
        ea.comment(content.getStringValue());
        ea.close();
        return ea.getConstructedNode();
    }

    @Override
    public XDMNode createDocument(CloseableIterator content) throws SystemException {
        DTMBuildingEventAcceptor ea = new DTMBuildingEventAcceptor(nameCache, avf);
        ea.open();
        ea.startDocument();
        try {
            XDMValue v;
            while ((v = (XDMValue) content.next()) != null) {
                switch (v.getDMOKind()) {
                    case SEQUENCE:
                        CloseableIterator ii = ((XDMSequence) v).createItemIterator();
                        try {
                            XDMItem i;
                            while ((i = (XDMItem) ii.next()) != null) {
                                ea.item(i);
                            }
                        } finally {
                            ii.close();
                        }
                        break;

                    default:
                        ea.item((XDMItem) v);
                        break;
                }
            }
        } finally {
            content.close();
        }
        ea.endDocument();
        ea.close();
        return ea.getConstructedNode();
    }

    @Override
    public XDMNode createElement(QNameValue name, QNameValue typeAnnotation, CloseableIterator content)
            throws SystemException {
        DTMBuildingEventAcceptor ea = new DTMBuildingEventAcceptor(nameCache, avf);
        ea.open();
        ea.startElement(name.getNameCache(), name.getCode());
        try {
            XDMItem item;
            while ((item = (XDMItem) content.next()) != null) {
                ea.item(item);
            }
        } finally {
            content.close();
        }
        ea.endElement();
        ea.close();
        return ea.getConstructedNode();
    }

    @Override
    public NodeConstructingEventAcceptor createElementConstructor() throws SystemException {
        return new DTMBuildingEventAcceptor(nameCache, avf);
    }

    @Override
    public XDMNode createProcessingInstruction(CharSequence target, CharSequence content) throws SystemException {
        DTMBuildingEventAcceptor ea = new DTMBuildingEventAcceptor(nameCache, avf);
        ea.open();
        ea.pi(nameCache, nameCache.intern("", "", target.toString()), content);
        ea.close();
        return ea.getConstructedNode();
    }

    @Override
    public XDMNode createProcessingInstruction(QNameValue target, StringValue content) throws SystemException {
        DTMBuildingEventAcceptor ea = new DTMBuildingEventAcceptor(nameCache, avf);
        ea.open();
        ea.pi(target.getNameCache(), target.getCode(), content.getStringValue());
        ea.close();
        return ea.getConstructedNode();
    }

    @Override
    public XDMNode createText(StringValue content) throws SystemException {
        DTMBuildingEventAcceptor ea = new DTMBuildingEventAcceptor(nameCache, avf);
        ea.open();
        ea.text(content.getStringValue());
        ea.close();
        return ea.getConstructedNode();
    }

    @Override
    public NodeConstructingEventAcceptor createDocumentConstructor() throws SystemException {
        return new DTMBuildingEventAcceptor(nameCache, avf);
    }

    @Override
    public XDMNode createText(CharSequence content) throws SystemException {
        DTMBuildingEventAcceptor ea = new DTMBuildingEventAcceptor(nameCache, avf);
        ea.open();
        ea.text(content);
        ea.close();
        return ea.getConstructedNode();
    }

    @Override
    public XDMNode createComment(CharSequence content) throws SystemException {
        DTMBuildingEventAcceptor ea = new DTMBuildingEventAcceptor(nameCache, avf);
        ea.open();
        ea.comment(content);
        ea.close();
        return ea.getConstructedNode();
    }
}