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
package org.apache.vxquery.context;

import javax.xml.namespace.QName;

import org.apache.vxquery.types.SequenceType;

import org.apache.hyracks.algebricks.core.algebra.base.LogicalVariable;

public class XQueryVariable {
    private final QName name;

    private final SequenceType type;

    private final LogicalVariable var;

    public XQueryVariable(QName name, SequenceType type, LogicalVariable var) {
        this.name = name;
        this.type = type;
        this.var = var;
    }

    public QName getName() {
        return name;
    }

    public SequenceType getType() {
        return type;
    }

    public LogicalVariable getLogicalVariable() {
        return var;
    }
}
