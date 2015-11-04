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

public class ForVarDeclNode extends ASTNode {
    private QNameNode forVar;
    private TypeDeclNode type;
    private ASTNode sequence;
    private QNameNode posVar;
    private QNameNode scoreVar;

    public ForVarDeclNode(SourceLocation loc) {
        super(loc);
    }

    @Override
    public ASTTag getTag() {
        return ASTTag.FOR_VARIABLE_DECLARATION;
    }

    public QNameNode getForVar() {
        return forVar;
    }

    public void setForVar(QNameNode forVar) {
        this.forVar = forVar;
    }

    public TypeDeclNode getType() {
        return type;
    }

    public void setType(TypeDeclNode type) {
        this.type = type;
    }

    public ASTNode getSequence() {
        return sequence;
    }

    public void setSequence(ASTNode sequence) {
        this.sequence = sequence;
    }

    public QNameNode getPosVar() {
        return posVar;
    }

    public void setPosVar(QNameNode posVar) {
        this.posVar = posVar;
    }

    public QNameNode getScoreVar() {
        return scoreVar;
    }

    public void setScoreVar(QNameNode scoreVar) {
        this.scoreVar = scoreVar;
    }
}