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

import edu.uci.ics.hyracks.algebricks.core.algebra.functions.FunctionIdentifier;
import edu.uci.ics.hyracks.algebricks.core.algebra.functions.IFunctionInfo;

public abstract class Function implements IFunctionInfo {
    private static final String VXQUERY = "vxquery";
    protected final FunctionIdentifier fid;

    protected final QName qname;

    protected final Signature signature;

    public Function(QName qname, Signature signature) {
        this.fid = new FunctionIdentifier(VXQUERY, "{" + qname.getNamespaceURI() + "}" + qname.getLocalPart(), false);
        this.qname = qname;
        this.signature = signature;
    }

    @Override
    public final FunctionIdentifier getFunctionIdentifier() {
        return fid;
    }

    public abstract FunctionTag getTag();

    public abstract boolean useContextImplicitly();

    public abstract FunctionIteratorFactory getIteratorFactory();

    public QName getName() {
        return qname;
    }

    public Signature getSignature() {
        return signature;
    }

    public enum FunctionTag {
        BUILTIN,
        OPERATOR,
        EXTERNAL,
        UDXQUERY,
    }

    @Override
    public Object getInfo() {
        return null;
    }
}