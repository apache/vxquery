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
package org.apache.vxquery.v0datamodel.dom;

import org.apache.vxquery.exceptions.SystemException;
import org.apache.vxquery.v0runtime.base.CloseableIterator;

final class DOMPrecedingSiblingsIterator implements CloseableIterator {
    private DOMNode focus;
    private DOMNode next;

    DOMPrecedingSiblingsIterator(DOMNode focus) {
        this.focus = focus;
        next = DOMNode.wrap(focus.getDOMManager(), focus.getWrappedObject().getPreviousSibling(), (DOMNode) focus
                .getParent());
    }

    @Override
    public void close() {
        next = null;
    }

    @Override
    public Object next() throws SystemException {
        if (next == null) {
            return null;
        }
        DOMNode oldNext = next;
        next = DOMNode.wrap(focus.getDOMManager(), next.getWrappedObject().getPreviousSibling(), (DOMNode) focus
                .getParent());
        return oldNext;
    }
}