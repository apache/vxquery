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
package org.apache.vxquery.xmlquery.ast;

import org.apache.vxquery.util.SourceLocation;

public class CaseClauseNode extends ASTNode {
    private QNameNode caseVar;
    private SequenceTypeNode type;
    private ASTNode valueExpr;

    public CaseClauseNode(SourceLocation loc) {
        super(loc);
    }

    @Override
    public ASTTag getTag() {
        return ASTTag.CASE_CLAUSE;
    }

    public QNameNode getCaseVar() {
        return caseVar;
    }

    public void setCaseVar(QNameNode caseVar) {
        this.caseVar = caseVar;
    }

    public SequenceTypeNode getType() {
        return type;
    }

    public void setType(SequenceTypeNode type) {
        this.type = type;
    }

    public ASTNode getValueExpr() {
        return valueExpr;
    }

    public void setValueExpr(ASTNode valueExpr) {
        this.valueExpr = valueExpr;
    }
}