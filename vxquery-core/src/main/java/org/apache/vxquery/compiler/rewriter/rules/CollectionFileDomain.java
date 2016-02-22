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
package org.apache.vxquery.compiler.rewriter.rules;

import edu.uci.ics.hyracks.algebricks.core.algebra.properties.INodeDomain;

public class CollectionFileDomain implements INodeDomain {

    private String collectionName;

    public CollectionFileDomain(String collectionName) {
        this.collectionName = collectionName;
    }

    @Override
    public Integer cardinality() {
        return null;
    }

    @Override
    public String toString() {
        return "CollectionFileDomain: " + collectionName;
    }

    @Override
    public boolean sameAs(INodeDomain domain) {
        if (!(domain instanceof CollectionFileDomain)) {
            return false;
        }
        CollectionFileDomain cfd = (CollectionFileDomain) domain;
        if (collectionName.compareTo(cfd.collectionName) != 0) {
            return false;
        }
        return true;
    }

}
