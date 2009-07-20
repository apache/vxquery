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
package org.apache.vxquery.types;

import org.apache.vxquery.datamodel.DMOKind;
import org.apache.vxquery.datamodel.NameCache;
import org.apache.vxquery.datamodel.XDMValue;
import org.apache.vxquery.exceptions.SystemException;
import org.apache.vxquery.util.Filter;

public final class TextType extends AbstractNodeType {
    public static final TextType INSTANCE = new TextType();

    private static final Filter<XDMValue> TEXT_FILTER = new Filter<XDMValue>() {
        @Override
        public boolean accept(XDMValue value) throws SystemException {
            if (value == null) {
                return false;
            }
            return value.getDMOKind() == DMOKind.TEXT_NODE;
        }
    };

    private TextType() {
    }

    @Override
    public NodeKind getNodeKind() {
        return NodeKind.TEXT;
    }

    @Override
    public Filter<XDMValue> createInstanceOfFilter(NameCache nameCache) {
        return TEXT_FILTER;
    }
}