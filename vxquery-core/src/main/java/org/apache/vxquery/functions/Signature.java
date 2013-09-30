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
package org.apache.vxquery.functions;

import javax.xml.namespace.QName;

import org.apache.commons.lang3.tuple.Pair;
import org.apache.vxquery.types.SequenceType;

public final class Signature {
    private QName[] parameterNames;
    private boolean varArgs;
    private SequenceType[] parameterTypes;
    private SequenceType rType;

    public Signature(SequenceType rType, Pair<QName, SequenceType>... parameters) {
        this(rType, false, parameters);
    }

    public Signature(SequenceType rType, boolean varArgs, Pair<QName, SequenceType>... parameters) {
        int len = parameters.length;
        this.varArgs = varArgs;
        parameterNames = new QName[len];
        parameterTypes = new SequenceType[len];
        for (int i = 0; i < parameters.length; ++i) {
            parameterNames[i] = parameters[i].getLeft();
            parameterTypes[i] = parameters[i].getRight();
        }
        this.rType = rType;
    }

    public boolean isVarArgs() {
        return varArgs;
    }

    public int getArity() {
        return parameterTypes.length;
    }

    public SequenceType getParameterType(int index) {
        if (varArgs && parameterTypes.length <= index) {
            return parameterTypes[parameterTypes.length - 1];
        }
        return parameterTypes[index];
    }

    public QName getParameterName(int index) {
        if (varArgs && parameterNames.length <= index) {
            return parameterNames[parameterNames.length - 1];
        }
        return parameterNames[index];
    }

    public SequenceType getReturnType() {
        return rType;
    }

    public void serialize(StringBuffer buffer) {
    }
}