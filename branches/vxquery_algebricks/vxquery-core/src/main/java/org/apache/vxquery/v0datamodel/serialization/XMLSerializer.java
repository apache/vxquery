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
package org.apache.vxquery.v0datamodel.serialization;

import java.io.IOException;
import java.io.Writer;
import java.util.Stack;

import org.apache.vxquery.exceptions.ErrorCode;
import org.apache.vxquery.exceptions.SystemException;
import org.apache.vxquery.v0datamodel.XDMItem;
import org.apache.vxquery.v0datamodel.XDMNode;
import org.apache.vxquery.v0datamodel.atomic.QNameValue;
import org.apache.vxquery.v0runtime.base.CloseableIterator;
import org.apache.vxquery.v0runtime.base.EventAcceptor;

public class XMLSerializer implements EventAcceptor {
    private static final String XML_DOCUMENT_HEADER = "<?xml version=\"1.0\" encoding=\"UTF-8\"?>";

    private final Writer out;
    private final boolean prettyPrint;
    private int level;
    private Stack<CharSequence> tagStack;

    private boolean elementTagOpen;
    private boolean lastItemText;

    public XMLSerializer(Writer out, boolean prettyPrint) {
        this.out = out;
        this.prettyPrint = prettyPrint;
        tagStack = new Stack<CharSequence>();
    }

    @Override
    public void open() throws SystemException {
        lastItemText = false;
        write(XML_DOCUMENT_HEADER);
    }

    @Override
    public void attribute(CharSequence uri, CharSequence localName, CharSequence prefix, CharSequence stringValue)
            throws SystemException {
        write(" ");
        write(localName);
        write("=\"");
        write(stringValue);
        write("\"");
    }

    @Override
    public void close() {
    }

    @Override
    public void comment(CharSequence content) throws SystemException {
        if (elementTagOpen) {
            closeElementTag();
        }
        write("<!--");
        write(content);
        write("-->");
        lastItemText = false;
    }

    @Override
    public void endDocument() throws SystemException {
        lastItemText = false;
    }

    @Override
    public void endElement() throws SystemException {
        if (elementTagOpen) {
            write("/>");
            tagStack.pop();
        } else {
            write("</");
            write(tagStack.pop());
            write(">");
        }
        elementTagOpen = false;
        lastItemText = false;
    }

    @Override
    public void item(XDMItem item) throws SystemException {
        switch (item.getDMOKind()) {
            case ATOMIC_VALUE:
                text(item.getStringValue());
                break;

            case ATTRIBUTE_NODE: {
                XDMNode aNode = (XDMNode) item;
                QNameValue name = aNode.getNodeName();
                attribute(name.getUri(), name.getLocalName(), name.getPrefix(), aNode.getStringValue());
                break;
            }

            case COMMENT_NODE:
                XDMNode cNode = (XDMNode) item;
                comment(cNode.getStringValue());
                break;

            case DOCUMENT_NODE:
                XDMNode dNode = (XDMNode) item;
                startDocument();
                writeItems(dNode.getChildren());
                endDocument();
                break;

            case ELEMENT_NODE: {
                XDMNode eNode = (XDMNode) item;
                QNameValue name = eNode.getNodeName();
                startElement(name.getUri(), name.getLocalName(), name.getPrefix());
                writeAttributes(eNode.getAttributes());
                writeItems(eNode.getChildren());
                endElement();
                break;
            }

            case PI_NODE: {
                XDMNode pNode = (XDMNode) item;
                QNameValue name = pNode.getNodeName();
                pi(name.getLocalName(), pNode.getStringValue());
                break;
            }

            case TEXT_NODE:
                XDMNode tNode = (XDMNode) item;
                text(tNode.getStringValue());
                break;

            default:
                throw new IllegalStateException();
        }
    }

    private void writeAttributes(CloseableIterator attributes) throws SystemException {
        XDMNode attr;
        while ((attr = (XDMNode) attributes.next()) != null) {
            QNameValue name = attr.getNodeName();
            attribute(name.getUri(), name.getLocalName(), name.getPrefix(), attr.getStringValue());
        }
        attributes.close();
    }

    private void writeItems(CloseableIterator children) throws SystemException {
        XDMItem item;
        while ((item = (XDMItem) children.next()) != null) {
            item(item);
        }
        children.close();
    }

    @Override
    public void namespace(CharSequence prefix, CharSequence uri) throws SystemException {
        lastItemText = false;
    }

    @Override
    public void pi(CharSequence target, CharSequence content) throws SystemException {
        if (elementTagOpen) {
            closeElementTag();
        }
        write("<?");
        write(target);
        if (content.length() > 0) {
            write(" ");
            write(content);
        }
        write("?>");
        lastItemText = false;
    }

    @Override
    public void startDocument() throws SystemException {
        lastItemText = false;
    }

    @Override
    public void startElement(CharSequence uri, CharSequence localName, CharSequence prefix) throws SystemException {
        if (elementTagOpen) {
            closeElementTag();
        }
        write("<");
        tagStack.push(localName);
        write(localName);
        elementTagOpen = true;
        lastItemText = false;
    }

    @Override
    public void text(CharSequence stringValue) throws SystemException {
        if (elementTagOpen) {
            closeElementTag();
        }
        if (lastItemText) {
            write(" ");
        }
        lastItemText = true;
        write(stringValue);
    }

    private void closeElementTag() throws SystemException {
        write(">");
        elementTagOpen = false;
        lastItemText = false;
    }

    private void write(CharSequence data) throws SystemException {
        try {
            out.append(data);
        } catch (IOException e) {
            e.printStackTrace();
            throw new SystemException(ErrorCode.SYSE0001);
        }
    }
}