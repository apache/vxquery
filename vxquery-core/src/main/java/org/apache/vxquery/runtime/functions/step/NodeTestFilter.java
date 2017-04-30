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
package org.apache.vxquery.runtime.functions.step;

import org.apache.hyracks.data.std.primitive.UTF8StringPointable;
import org.apache.vxquery.datamodel.accessors.TaggedValuePointable;
import org.apache.vxquery.datamodel.accessors.atomic.CodedQNamePointable;
import org.apache.vxquery.datamodel.accessors.nodes.AttributeNodePointable;
import org.apache.vxquery.datamodel.accessors.nodes.ElementNodePointable;
import org.apache.vxquery.datamodel.accessors.nodes.NodeTreePointable;
import org.apache.vxquery.datamodel.values.ValueTag;
import org.apache.vxquery.types.AttributeType;
import org.apache.vxquery.types.ElementType;
import org.apache.vxquery.types.NameTest;
import org.apache.vxquery.types.NodeType;
import org.apache.vxquery.types.SequenceType;

public class NodeTestFilter {

    public static INodeFilter getNodeTestFilter(SequenceType sType) {
        INodeFilter filter;
        final NodeType nodeType = (NodeType) sType.getItemType();
        switch (nodeType.getNodeKind()) {
            case ATTRIBUTE: {
                AttributeType aType = (AttributeType) nodeType;
                NameTest nameTest = aType.getNameTest();
                byte[] uri = nameTest.getUri();
                byte[] localName = nameTest.getLocalName();
                final UTF8StringPointable urip = (UTF8StringPointable) (uri == null ? null
                        : UTF8StringPointable.FACTORY.createPointable());
                final UTF8StringPointable localp = (UTF8StringPointable) (localName == null ? null
                        : UTF8StringPointable.FACTORY.createPointable());
                if (uri != null) {
                    urip.set(uri, 0, uri.length);
                }
                if (localName != null) {
                    localp.set(localName, 0, localName.length);
                }
                final UTF8StringPointable temp = (UTF8StringPointable) UTF8StringPointable.FACTORY.createPointable();
                final AttributeNodePointable anp = (AttributeNodePointable) AttributeNodePointable.FACTORY
                        .createPointable();
                final CodedQNamePointable cqp = (CodedQNamePointable) CodedQNamePointable.FACTORY.createPointable();
                filter = new INodeFilter() {
                    @Override
                    public boolean accept(NodeTreePointable ntp, TaggedValuePointable tvp) {
                        if (tvp.getTag() != ValueTag.ATTRIBUTE_NODE_TAG) {
                            return false;
                        }
                        tvp.getValue(anp);
                        anp.getName(cqp);
                        if (urip != null) {
                            ntp.getString(cqp.getNamespaceCode(), temp);
                            if (urip.compareTo(temp) != 0) {
                                return false;
                            }
                        }
                        if (localp != null) {
                            ntp.getString(cqp.getLocalCode(), temp);
                            if (localp.compareTo(temp) != 0) {
                                return false;
                            }
                        }
                        return true;
                    }
                };
                break;
            }

            case COMMENT:
                filter = new INodeFilter() {
                    @Override
                    public boolean accept(NodeTreePointable ntp, TaggedValuePointable tvp) {
                        return tvp.getTag() == ValueTag.COMMENT_NODE_TAG;
                    }
                };
                break;

            case DOCUMENT:
                filter = new INodeFilter() {
                    @Override
                    public boolean accept(NodeTreePointable ntp, TaggedValuePointable tvp) {
                        return tvp.getTag() == ValueTag.DOCUMENT_NODE_TAG;
                    }
                };
                break;

            case ELEMENT: {
                ElementType eType = (ElementType) nodeType;
                NameTest nameTest = eType.getNameTest();
                byte[] uri = nameTest.getUri();
                byte[] localName = nameTest.getLocalName();
                final UTF8StringPointable urip = (UTF8StringPointable) (uri == null ? null
                        : UTF8StringPointable.FACTORY.createPointable());
                final UTF8StringPointable localp = (UTF8StringPointable) (localName == null ? null
                        : UTF8StringPointable.FACTORY.createPointable());
                if (uri != null) {
                    urip.set(uri, 0, uri.length);
                }
                if (localName != null) {
                    localp.set(localName, 0, localName.length);
                }
                final UTF8StringPointable temp = (UTF8StringPointable) UTF8StringPointable.FACTORY.createPointable();
                final ElementNodePointable enp = (ElementNodePointable) ElementNodePointable.FACTORY.createPointable();
                final CodedQNamePointable cqp = (CodedQNamePointable) CodedQNamePointable.FACTORY.createPointable();
                filter = new INodeFilter() {
                    @Override
                    public boolean accept(NodeTreePointable ntp, TaggedValuePointable tvp) {
                        if (tvp.getTag() != ValueTag.ELEMENT_NODE_TAG) {
                            return false;
                        }
                        tvp.getValue(enp);
                        enp.getName(cqp);
                        if (urip != null) {
                            ntp.getString(cqp.getNamespaceCode(), temp);
                            if (urip.compareTo(temp) != 0) {
                                return false;
                            }
                        }
                        if (localp != null) {
                            ntp.getString(cqp.getLocalCode(), temp);
                            if (localp.compareTo(temp) != 0) {
                                return false;
                            }
                        }
                        return true;
                    }
                };
                break;
            }

            case PI:
                filter = new INodeFilter() {
                    @Override
                    public boolean accept(NodeTreePointable ntp, TaggedValuePointable tvp) {
                        return tvp.getTag() == ValueTag.PI_NODE_TAG;
                    }
                };
                break;

            case TEXT:
                filter = new INodeFilter() {
                    @Override
                    public boolean accept(NodeTreePointable ntp, TaggedValuePointable tvp) {
                        return tvp.getTag() == ValueTag.TEXT_NODE_TAG;
                    }
                };
                break;

            case ANY:
            default:
                filter = new INodeFilter() {
                    @Override
                    public boolean accept(NodeTreePointable ntp, TaggedValuePointable tvp) {
                        return true;
                    }
                };
                break;
        }
        return filter;
    }

    public interface INodeFilter {
        public boolean accept(NodeTreePointable ntp, TaggedValuePointable tvp);
    }
}
