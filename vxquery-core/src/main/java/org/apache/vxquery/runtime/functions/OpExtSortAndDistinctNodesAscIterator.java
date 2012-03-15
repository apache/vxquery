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
package org.apache.vxquery.runtime.functions;

import java.util.Comparator;

import org.apache.vxquery.context.StaticContext;
import org.apache.vxquery.functions.Function;
import org.apache.vxquery.runtime.RegisterAllocator;
import org.apache.vxquery.runtime.base.RuntimeIterator;
import org.apache.vxquery.v0datamodel.XDMItem;
import org.apache.vxquery.v0datamodel.XDMNode;
import org.apache.vxquery.v0datamodel.atomic.compare.ComparisonUtils;

public class OpExtSortAndDistinctNodesAscIterator extends AbstractItemSortingAndDistinctIterator {
    public OpExtSortAndDistinctNodesAscIterator(RegisterAllocator rAllocator, Function fn, RuntimeIterator[] arguments,
            StaticContext ctx) {
        super(rAllocator, fn, arguments, ctx);
    }

    @Override
    protected boolean itemsEqual(XDMItem i0, XDMItem i1) {
        return ((XDMNode) i0).isSameNode((XDMNode) i1);
    }

    @Override
    protected Comparator<XDMItem> getComparator() {
        return ComparisonUtils.SORT_NODE_ASC_COMPARATOR;
    }
}