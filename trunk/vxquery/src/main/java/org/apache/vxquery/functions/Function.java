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

import org.apache.vxquery.runtime.base.FunctionIteratorFactory;

public abstract class Function {
    protected QName name;

    protected Signature signature;

    public Function(QName name, Signature signature) {
        this.name = name;
        this.signature = signature;
    }

    public abstract FunctionTag getTag();

    public abstract boolean useContextImplicitly();

    public abstract FunctionIteratorFactory getIteratorFactory();

    public QName getName() {
        return name;
    }

    public Signature getSignature() {
        return signature;
    }

    public enum FunctionTag {
        BUILTIN, OPERATOR, EXTERNAL, UDXQUERY,
    }
}