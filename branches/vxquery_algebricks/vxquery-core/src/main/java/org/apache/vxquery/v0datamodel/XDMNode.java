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
package org.apache.vxquery.v0datamodel;

import org.apache.vxquery.v0datamodel.atomic.AnyUriValue;
import org.apache.vxquery.v0datamodel.atomic.QNameValue;
import org.apache.vxquery.v0runtime.base.CloseableIterator;

public interface XDMNode extends XDMItem {
    public Object getImplementationIdentifier();

    public AnyUriValue getBaseUri();

    public AnyUriValue getDocumentUri();

    public boolean isID();

    public boolean isIDREFS();

    public boolean isSameNode(XDMNode other);

    public int compareDocumentOrder(XDMNode other);

    public QNameValue getNodeName();

    public XDMNode getParent();

    public boolean getIsNilled();

    public XDMValue getTypedValue();

    public boolean hasChildren();

    public boolean hasAttributes();

    public CloseableIterator getAttributes();

    public CloseableIterator getChildren();

    public CloseableIterator getFollowingSiblings();

    public CloseableIterator getPrecedingSiblings();
}