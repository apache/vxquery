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

import org.apache.vxquery.compiler.rewriter.rules.propagationpolicies.IPropertyPropagationPolicy;
import org.apache.vxquery.compiler.rewriter.rules.propagationpolicies.documentorder.DocumentOrder;
import org.apache.vxquery.compiler.rewriter.rules.propagationpolicies.documentorder.DocumentOrderYESPropagationPolicy;
import org.apache.vxquery.compiler.rewriter.rules.propagationpolicies.uniquenodes.UniqueNodes;
import org.apache.vxquery.compiler.rewriter.rules.propagationpolicies.uniquenodes.UniqueNodesYESPropagationPolicy;
import org.apache.vxquery.exceptions.ErrorCode;
import org.apache.vxquery.exceptions.SystemException;

import org.apache.hyracks.algebricks.core.algebra.functions.FunctionIdentifier;
import org.apache.hyracks.algebricks.core.algebra.functions.IFunctionInfo;
import org.apache.hyracks.algebricks.runtime.base.IAggregateEvaluatorFactory;
import org.apache.hyracks.algebricks.runtime.base.IScalarEvaluatorFactory;
import org.apache.hyracks.algebricks.runtime.base.IUnnestingEvaluatorFactory;

public abstract class Function implements IFunctionInfo {
    private static final String VXQUERY = "vxquery";
    protected final FunctionIdentifier fid;

    protected final QName qname;

    protected final Signature signature;

    protected IPropertyPropagationPolicy<DocumentOrder> documentOrderPropagationPolicy;
    protected IPropertyPropagationPolicy<UniqueNodes> uniqueNodesPropagationPolicy;

    protected boolean aggregateEvaluatorFactory = false;
    protected boolean scalarEvaluatorFactory = false;
    protected boolean unnestingEvaluatorFactory = false;

    public Function(QName qname, Signature signature) {
        this.fid = new FunctionIdentifier(VXQUERY, "{" + qname.getNamespaceURI() + "}" + qname.getLocalPart());
        this.qname = qname;
        this.signature = signature;
        this.documentOrderPropagationPolicy = new DocumentOrderYESPropagationPolicy();
        this.uniqueNodesPropagationPolicy = new UniqueNodesYESPropagationPolicy();
    }

    @Override
    public final FunctionIdentifier getFunctionIdentifier() {
        return fid;
    }

    public abstract FunctionTag getTag();

    public abstract boolean useContextImplicitly();

    public boolean isFunctional() {
        return true;
    }

    public IScalarEvaluatorFactory createScalarEvaluatorFactory(IScalarEvaluatorFactory[] args) throws SystemException {
        throw new SystemException(ErrorCode.SYSE0001, "No IScalarEvaluatorFactory runtime for " + fid.getName());
    }

    public IAggregateEvaluatorFactory createAggregateEvaluatorFactory(IScalarEvaluatorFactory[] args)
            throws SystemException {
        throw new SystemException(ErrorCode.SYSE0001, "No IAggregateEvaluatorFactory runtime for " + fid.getName());
    }

    public IUnnestingEvaluatorFactory createUnnestingEvaluatorFactory(IScalarEvaluatorFactory[] args)
            throws SystemException {
        throw new SystemException(ErrorCode.SYSE0001, "No IUnnestingEvaluatorFactory runtime for " + fid.getName());
    }

    public IPropertyPropagationPolicy<DocumentOrder> getDocumentOrderPropagationPolicy() {
        return this.documentOrderPropagationPolicy;
    }

    public IPropertyPropagationPolicy<UniqueNodes> getUniqueNodesPropagationPolicy() {
        return this.uniqueNodesPropagationPolicy;
    }

    public boolean hasAggregateEvaluatorFactory() {
        return this.aggregateEvaluatorFactory;
    }

    public boolean hasScalarEvaluatorFactory() {
        return this.scalarEvaluatorFactory;
    }

    public boolean hasUnnestingEvaluatorFactory() {
        return this.unnestingEvaluatorFactory;
    }

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
}
