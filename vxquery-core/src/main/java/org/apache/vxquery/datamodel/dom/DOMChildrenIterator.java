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
package org.apache.vxquery.datamodel.dom;

import org.w3c.dom.NodeList;

import org.apache.vxquery.exceptions.SystemException;
import org.apache.vxquery.runtime.base.CloseableIterator;

final class DOMChildrenIterator implements CloseableIterator {
    private DOMNode parent;
    private NodeList children;
    private int index;

    DOMChildrenIterator(DOMNode parent) {
        this.parent = parent;
        children = parent.getWrappedObject().getChildNodes();
        index = 0;
    }

    @Override
    public void close() {
        children = null;
    }

    @Override
    public Object next() throws SystemException {
        if (children == null || index >= children.getLength()) {
            return null;
        }
        return DOMNode.wrap(parent.getDOMManager(), children.item(index++), parent);
    }
}