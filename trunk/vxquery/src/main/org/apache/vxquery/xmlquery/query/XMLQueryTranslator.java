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
package org.apache.vxquery.xmlquery.query;

import java.math.BigDecimal;
import java.math.BigInteger;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import javax.xml.namespace.QName;

import org.apache.vxquery.compiler.CompilerControlBlock;
import org.apache.vxquery.compiler.expression.AttributeNodeConstructorExpression;
import org.apache.vxquery.compiler.expression.CastExpression;
import org.apache.vxquery.compiler.expression.CastableExpression;
import org.apache.vxquery.compiler.expression.CommentNodeConstructorExpression;
import org.apache.vxquery.compiler.expression.ConstantExpression;
import org.apache.vxquery.compiler.expression.DocumentNodeConstructorExpression;
import org.apache.vxquery.compiler.expression.ElementNodeConstructorExpression;
import org.apache.vxquery.compiler.expression.Expression;
import org.apache.vxquery.compiler.expression.ExpressionBuilder;
import org.apache.vxquery.compiler.expression.FLWORExpression;
import org.apache.vxquery.compiler.expression.ForLetVariable;
import org.apache.vxquery.compiler.expression.FunctionCallExpression;
import org.apache.vxquery.compiler.expression.GlobalVariable;
import org.apache.vxquery.compiler.expression.IfThenElseExpression;
import org.apache.vxquery.compiler.expression.InstanceofExpression;
import org.apache.vxquery.compiler.expression.PINodeConstructorExpression;
import org.apache.vxquery.compiler.expression.ParameterVariable;
import org.apache.vxquery.compiler.expression.PathStepExpression;
import org.apache.vxquery.compiler.expression.PositionVariable;
import org.apache.vxquery.compiler.expression.PromoteExpression;
import org.apache.vxquery.compiler.expression.QuantifiedExpression;
import org.apache.vxquery.compiler.expression.ScoreVariable;
import org.apache.vxquery.compiler.expression.TextNodeConstructorExpression;
import org.apache.vxquery.compiler.expression.TreatExpression;
import org.apache.vxquery.compiler.expression.TypeswitchExpression;
import org.apache.vxquery.compiler.expression.ValidateExpression;
import org.apache.vxquery.compiler.expression.Variable;
import org.apache.vxquery.compiler.expression.VariableReferenceExpression;
import org.apache.vxquery.compiler.expression.Variable.VarTag;
import org.apache.vxquery.context.StaticContext;
import org.apache.vxquery.context.StaticContextImpl;
import org.apache.vxquery.context.ThinStaticContextImpl;
import org.apache.vxquery.datamodel.AxisKind;
import org.apache.vxquery.datamodel.atomic.AtomicValueFactory;
import org.apache.vxquery.exceptions.ErrorCode;
import org.apache.vxquery.exceptions.SystemException;
import org.apache.vxquery.functions.BuiltinFunctions;
import org.apache.vxquery.functions.BuiltinOperators;
import org.apache.vxquery.functions.ExternalFunction;
import org.apache.vxquery.functions.Function;
import org.apache.vxquery.functions.Signature;
import org.apache.vxquery.functions.UserDefinedXQueryFunction;
import org.apache.vxquery.types.AnyItemType;
import org.apache.vxquery.types.AnyNodeType;
import org.apache.vxquery.types.AnyType;
import org.apache.vxquery.types.AtomicType;
import org.apache.vxquery.types.AttributeType;
import org.apache.vxquery.types.BuiltinTypeRegistry;
import org.apache.vxquery.types.CommentType;
import org.apache.vxquery.types.DocumentType;
import org.apache.vxquery.types.ElementType;
import org.apache.vxquery.types.EmptySequenceType;
import org.apache.vxquery.types.ItemType;
import org.apache.vxquery.types.NameTest;
import org.apache.vxquery.types.NodeType;
import org.apache.vxquery.types.ProcessingInstructionType;
import org.apache.vxquery.types.Quantifier;
import org.apache.vxquery.types.SchemaType;
import org.apache.vxquery.types.SequenceType;
import org.apache.vxquery.types.TextType;
import org.apache.vxquery.types.TypeUtils;
import org.apache.vxquery.util.Pair;
import org.apache.vxquery.xmlquery.ast.ASTNode;
import org.apache.vxquery.xmlquery.ast.ASTTag;
import org.apache.vxquery.xmlquery.ast.AtomicTypeNode;
import org.apache.vxquery.xmlquery.ast.AttributeTestNode;
import org.apache.vxquery.xmlquery.ast.AxisStepNode;
import org.apache.vxquery.xmlquery.ast.BaseUriDeclNode;
import org.apache.vxquery.xmlquery.ast.BoundarySpaceDeclNode;
import org.apache.vxquery.xmlquery.ast.CDataSectionNode;
import org.apache.vxquery.xmlquery.ast.CaseClauseNode;
import org.apache.vxquery.xmlquery.ast.ComputedAttributeConstructorNode;
import org.apache.vxquery.xmlquery.ast.ComputedCommentConstructorNode;
import org.apache.vxquery.xmlquery.ast.ComputedDocumentConstructorNode;
import org.apache.vxquery.xmlquery.ast.ComputedElementConstructorNode;
import org.apache.vxquery.xmlquery.ast.ComputedPIConstructorNode;
import org.apache.vxquery.xmlquery.ast.ComputedTextConstructorNode;
import org.apache.vxquery.xmlquery.ast.ConstructionDeclNode;
import org.apache.vxquery.xmlquery.ast.ContentCharsNode;
import org.apache.vxquery.xmlquery.ast.CopyNamespacesDeclNode;
import org.apache.vxquery.xmlquery.ast.DefaultCollationDeclNode;
import org.apache.vxquery.xmlquery.ast.DefaultElementNamespaceDeclNode;
import org.apache.vxquery.xmlquery.ast.DefaultFunctionNamespaceDeclNode;
import org.apache.vxquery.xmlquery.ast.DirectAttributeConstructorNode;
import org.apache.vxquery.xmlquery.ast.DirectCommentConstructorNode;
import org.apache.vxquery.xmlquery.ast.DirectElementConstructorNode;
import org.apache.vxquery.xmlquery.ast.DirectPIConstructorNode;
import org.apache.vxquery.xmlquery.ast.DocumentTestNode;
import org.apache.vxquery.xmlquery.ast.ElementTestNode;
import org.apache.vxquery.xmlquery.ast.EmptyOrderDeclNode;
import org.apache.vxquery.xmlquery.ast.EnclosedExprNode;
import org.apache.vxquery.xmlquery.ast.ExprNode;
import org.apache.vxquery.xmlquery.ast.ExtensionExprNode;
import org.apache.vxquery.xmlquery.ast.FLWORClauseNode;
import org.apache.vxquery.xmlquery.ast.FLWORExprNode;
import org.apache.vxquery.xmlquery.ast.FilterExprNode;
import org.apache.vxquery.xmlquery.ast.ForClauseNode;
import org.apache.vxquery.xmlquery.ast.ForVarDeclNode;
import org.apache.vxquery.xmlquery.ast.FunctionDeclNode;
import org.apache.vxquery.xmlquery.ast.FunctionExprNode;
import org.apache.vxquery.xmlquery.ast.IfExprNode;
import org.apache.vxquery.xmlquery.ast.InfixExprNode;
import org.apache.vxquery.xmlquery.ast.LetClauseNode;
import org.apache.vxquery.xmlquery.ast.LetVarDeclNode;
import org.apache.vxquery.xmlquery.ast.LibraryModuleNode;
import org.apache.vxquery.xmlquery.ast.LiteralNode;
import org.apache.vxquery.xmlquery.ast.MainModuleNode;
import org.apache.vxquery.xmlquery.ast.ModuleImportNode;
import org.apache.vxquery.xmlquery.ast.ModuleNode;
import org.apache.vxquery.xmlquery.ast.NCNameNode;
import org.apache.vxquery.xmlquery.ast.NameTestNode;
import org.apache.vxquery.xmlquery.ast.NamespaceDeclNode;
import org.apache.vxquery.xmlquery.ast.OptionDeclNode;
import org.apache.vxquery.xmlquery.ast.OrderSpecNode;
import org.apache.vxquery.xmlquery.ast.OrderbyClauseNode;
import org.apache.vxquery.xmlquery.ast.OrderedExprNode;
import org.apache.vxquery.xmlquery.ast.OrderingModeDeclNode;
import org.apache.vxquery.xmlquery.ast.PITestNode;
import org.apache.vxquery.xmlquery.ast.ParamNode;
import org.apache.vxquery.xmlquery.ast.ParenthesizedExprNode;
import org.apache.vxquery.xmlquery.ast.PathExprNode;
import org.apache.vxquery.xmlquery.ast.PrologNode;
import org.apache.vxquery.xmlquery.ast.QNameNode;
import org.apache.vxquery.xmlquery.ast.QuantifiedExprNode;
import org.apache.vxquery.xmlquery.ast.QuantifiedVarDeclNode;
import org.apache.vxquery.xmlquery.ast.QueryBodyNode;
import org.apache.vxquery.xmlquery.ast.RelativePathExprNode;
import org.apache.vxquery.xmlquery.ast.SchemaImportNode;
import org.apache.vxquery.xmlquery.ast.SequenceTypeNode;
import org.apache.vxquery.xmlquery.ast.SingleTypeNode;
import org.apache.vxquery.xmlquery.ast.TypeDeclNode;
import org.apache.vxquery.xmlquery.ast.TypeExprNode;
import org.apache.vxquery.xmlquery.ast.TypeswitchExprNode;
import org.apache.vxquery.xmlquery.ast.UnaryExprNode;
import org.apache.vxquery.xmlquery.ast.UnorderedExprNode;
import org.apache.vxquery.xmlquery.ast.ValidateExprNode;
import org.apache.vxquery.xmlquery.ast.VarDeclNode;
import org.apache.vxquery.xmlquery.ast.VarRefNode;
import org.apache.vxquery.xmlquery.ast.VersionDeclNode;
import org.apache.vxquery.xmlquery.ast.WhereClauseNode;
import org.apache.vxquery.xmlquery.ast.InfixExprNode.InfixOperator;
import org.apache.vxquery.xmlquery.query.XQueryConstants.PathType;
import org.apache.vxquery.xmlquery.query.XQueryConstants.TypeQuantifier;

final class XMLQueryTranslator {
    private CompilerControlBlock ccb;

    private StaticContext rootCtx;

    private StaticContext moduleCtx;

    private StaticContext currCtx;

    private VariableScope varScope;

    private int varCounter;

    XMLQueryTranslator(CompilerControlBlock ccb) {
        this.ccb = ccb;
        rootCtx = ccb.getStaticContext();
    }

    private void pushVariableScope() {
        final VariableScope parent = varScope;
        varScope = new VariableScope() {
            private Map<QName, Variable> varMap = new HashMap<QName, Variable>();

            @Override
            public VariableScope getParentScope() {
                return parent;
            }

            @Override
            public Variable lookupVariable(QName name) {
                if (varMap.containsKey(name)) {
                    return varMap.get(name);
                }
                return parent.lookupVariable(name);
            }

            @Override
            public void registerVariable(Variable var) {
                varMap.put(var.getName(), var);
            }
        };
    }

    private void popVariableScope() {
        varScope = varScope.getParentScope();
    }

    private void pushContext() {
        currCtx = new ThinStaticContextImpl(currCtx);
    }

    private void popContext() {
        currCtx = currCtx.getParent();
    }

    private QName createQName(QNameNode qnNode) throws SystemException {
        return createQName(qnNode, "");
    }

    private QName createQName(QNameNode qnNode, String defaultUri) throws SystemException {
        String prefix = qnNode.getPrefix();
        String local = qnNode.getLocalName();

        String uri;
        if (!"".equals(prefix)) {
            uri = currCtx.lookupNamespaceUri(prefix);
            if (uri == null) {
                throw new SystemException(ErrorCode.XPST0081, qnNode.getSourceLocation());
            }
        } else {
            uri = defaultUri;
        }
        return new QName(uri, local, prefix);
    }

    Module translateModule(ModuleNode moduleNode) throws SystemException {
        Module module = new Module();

        moduleCtx = new StaticContextImpl(rootCtx);
        varScope = new VariableScope() {
            @Override
            public VariableScope getParentScope() {
                return null;
            }

            @Override
            public Variable lookupVariable(QName name) {
                return moduleCtx.lookupVariable(name);
            }

            @Override
            public void registerVariable(Variable var) {
                moduleCtx.registerVariable(var);
            }
        };
        currCtx = moduleCtx;
        module.setModuleContext(moduleCtx);
        module.setCompilerControlBlock(ccb);

        VersionDeclNode ver = moduleNode.getVersion();
        if (ver != null) {
            if (!"1.0".equals(ver.getVersion())) {
                throw new SystemException(ErrorCode.XQST0031, ver.getSourceLocation());
            }
        }

        switch (moduleNode.getTag()) {
            case LIBRARY_MODULE: {
                module.setModuleType(ModuleType.LIBRARY);
                LibraryModuleNode lmNode = (LibraryModuleNode) moduleNode;
                String prefix = lmNode.getModuleDecl().getPrefix();
                String uri = unquote(lmNode.getModuleDecl().getTargetNS());
                if (prefix != null) {
                    currCtx.registerNamespaceUri(prefix, uri);
                }
                module.setNamespaceUri(uri);
                break;
            }

            case MAIN_MODULE: {
                module.setModuleType(ModuleType.MAIN);
                break;
            }

            default:
                throw new IllegalStateException("Unknown module type: " + moduleNode.getTag());
        }

        PrologNode prologNode = moduleNode.getProlog();
        parsePrologPass1(prologNode);
        parsePrologPass2(prologNode);

        switch (moduleNode.getTag()) {
            case LIBRARY_MODULE: {
                break;
            }

            case MAIN_MODULE: {
                MainModuleNode mmNode = (MainModuleNode) moduleNode;
                QueryBodyNode qbNode = mmNode.getQueryBody();
                module.setBody(translateExpression(qbNode.getExpression()));
                break;
            }

            default:
                throw new IllegalStateException("Unknown module type: " + moduleNode.getTag());
        }

        return module;
    }

    @SuppressWarnings("unchecked")
    private void parsePrologPass1(PrologNode prologNode) throws SystemException {
        if (prologNode != null) {
            List<ASTNode> decls = prologNode.getDecls();
            for (ASTNode d : decls) {
                switch (d.getTag()) {
                    case DEFAULT_ELEMENT_NAMESPACE_DECLARATION: {
                        DefaultElementNamespaceDeclNode node = (DefaultElementNamespaceDeclNode) d;
                        moduleCtx.setDefaultElementNamespaceUri(node.getUri());
                        break;
                    }

                    case DEFAULT_FUNCTION_NAMESPACE_DECLARATION: {
                        DefaultFunctionNamespaceDeclNode node = (DefaultFunctionNamespaceDeclNode) d;
                        moduleCtx.setDefaultFunctionNamespaceUri(node.getUri());
                        break;
                    }

                    case BOUNDARY_SPACE_DECLARATION: {
                        BoundarySpaceDeclNode node = (BoundarySpaceDeclNode) d;
                        moduleCtx.setBoundarySpaceProperty(node.getMode());
                        break;
                    }

                    case DEFAULT_COLLATION_DECLARATION: {
                        DefaultCollationDeclNode node = (DefaultCollationDeclNode) d;
                        moduleCtx.setDefaultCollation(node.getCollation());
                        break;
                    }

                    case BASE_URI_DECLARATION: {
                        BaseUriDeclNode node = (BaseUriDeclNode) d;
                        moduleCtx.setBaseUri(node.getUri());
                        break;
                    }

                    case CONSTRUCTION_DECLARATION: {
                        ConstructionDeclNode node = (ConstructionDeclNode) d;
                        moduleCtx.setConstructionModeProperty(node.getMode());
                        break;
                    }

                    case ORDERING_MODE_DECLARATION: {
                        OrderingModeDeclNode node = (OrderingModeDeclNode) d;
                        moduleCtx.setOrderingModeProperty(node.getMode());
                        break;
                    }

                    case EMPTY_ORDER_DECLARATION: {
                        EmptyOrderDeclNode node = (EmptyOrderDeclNode) d;
                        moduleCtx.setEmptyOrderProperty(node.getMode());
                        break;
                    }

                    case COPY_NAMESPACES_DECLARATION: {
                        CopyNamespacesDeclNode node = (CopyNamespacesDeclNode) d;
                        moduleCtx.setCopyNamespacesModeProperty(node.getMode());
                        break;
                    }

                    case NAMESPACE_DECLARATION: {
                        NamespaceDeclNode node = (NamespaceDeclNode) d;
                        moduleCtx.registerNamespaceUri(node.getPrefix(), unquote(node.getUri()));
                        break;
                    }

                    case SCHEMA_IMPORT: {
                        SchemaImportNode node = (SchemaImportNode) d;
                        if (node.isDefaultElementNamespace()) {
                            moduleCtx.setDefaultElementNamespaceUri(node.getTargetNS());
                        }
                        if (node.getPrefix() != null) {
                            moduleCtx.registerNamespaceUri(node.getPrefix(), unquote(node.getTargetNS()));
                        }
                        moduleCtx.registerSchemaImport(node.getTargetNS(), node.getLocations());
                        break;
                    }

                    case MODULE_IMPORT: {
                        ModuleImportNode node = (ModuleImportNode) d;
                        if (node.getPrefix() != null) {
                            moduleCtx.registerNamespaceUri(node.getPrefix(), unquote(node.getTargetNS()));
                        }
                        moduleCtx.registerModuleImport(node.getTargetNS(), node.getLocations());
                        break;
                    }

                    case VARIABLE_DECLARATION: {
                        VarDeclNode node = (VarDeclNode) d;
                        QName name = createQName(node.getName());
                        Variable var;
                        var = new GlobalVariable(name);
                        SequenceType type = SequenceType.create(AnyItemType.INSTANCE, Quantifier.QUANT_STAR);
                        if (node.getType() != null) {
                            type = createSequenceType(node.getType());
                        }
                        var.setDeclaredStaticType(type);
                        moduleCtx.registerVariable(var);
                        break;
                    }

                    case FUNCTION_DECLARATION: {
                        FunctionDeclNode node = (FunctionDeclNode) d;
                        boolean external = node.getBody() == null;
                        QName name = createQName(node.getName(), moduleCtx.getDefaultFunctionNamespaceUri());
                        String uri = name.getNamespaceURI();
                        if (XQueryConstants.FN_NSURI.equals(uri) || XQueryConstants.XS_NSURI.equals(uri)
                                || XQueryConstants.XSI_NSURI.equals(uri) || XQueryConstants.XML_NSURI.equals(uri)) {
                            throw new SystemException(ErrorCode.XQST0045, node.getSourceLocation());
                        }
                        SequenceType rType = SequenceType.create(AnyItemType.INSTANCE, Quantifier.QUANT_STAR);
                        if (node.getReturnType() != null) {
                            rType = createSequenceType(node.getReturnType());
                        }
                        Pair<QName, SequenceType> paramTypes[] = new Pair[node.getParameters().size()];
                        for (int i = 0; i < paramTypes.length; ++i) {
                            ParamNode pNode = node.getParameters().get(i);
                            QName pName = createQName(pNode.getName());
                            SequenceType pType = SequenceType.create(AnyItemType.INSTANCE, Quantifier.QUANT_STAR);
                            if (pNode.getType() != null) {
                                pType = createSequenceType(pNode.getType());
                            }
                            paramTypes[i] = new Pair<QName, SequenceType>(pName, pType);
                        }
                        Signature sign = new Signature(rType, paramTypes);
                        Function f = external ? new ExternalFunction(name, sign) : new UserDefinedXQueryFunction(name,
                                sign, null);
                        moduleCtx.registerFunction(f);
                        break;
                    }

                    case OPTION_DECLARATION: {
                        OptionDeclNode node = (OptionDeclNode) d;
                        QName name = createQName(node.getName());
                        moduleCtx.setOption(name, node.getValue());
                        break;
                    }

                    default:
                        throw new IllegalStateException("Unknown node: " + d.getTag());
                }
            }
        }
    }

    private void parsePrologPass2(PrologNode prologNode) throws SystemException {
        if (prologNode != null) {
            List<ASTNode> decls = prologNode.getDecls();
            for (ASTNode d : decls) {
                switch (d.getTag()) {
                    case VARIABLE_DECLARATION: {
                        VarDeclNode node = (VarDeclNode) d;
                        QName name = createQName(node.getName());
                        Variable var = moduleCtx.lookupVariable(name);
                        if (node.getValue() != null) {
                            Expression value = translateExpression(node.getValue());
                            value = new TreatExpression(currCtx, value, var.getDeclaredStaticType());
                            ((GlobalVariable) var).setInitializerExpression(value);
                        }
                        break;
                    }

                    case FUNCTION_DECLARATION: {
                        FunctionDeclNode node = (FunctionDeclNode) d;
                        boolean external = node.getBody() == null;
                        if (!external) {
                            QName name = createQName(node.getName(), moduleCtx.getDefaultFunctionNamespaceUri());
                            int arity = node.getParameters().size();
                            UserDefinedXQueryFunction f = (UserDefinedXQueryFunction) moduleCtx.lookupFunction(name,
                                    arity);
                            Signature sign = f.getSignature();
                            pushVariableScope();
                            ParameterVariable[] params = new ParameterVariable[arity];
                            for (int i = 0; i < arity; ++i) {
                                ParamNode pNode = node.getParameters().get(i);
                                QName pName = createQName(pNode.getName());
                                SequenceType pType = SequenceType.create(AnyItemType.INSTANCE, Quantifier.QUANT_STAR);
                                if (pNode.getType() != null) {
                                    pType = createSequenceType(pNode.getType());
                                }
                                ParameterVariable pVar = new ParameterVariable(pName);
                                pVar.setDeclaredStaticType(pType);
                                params[i] = pVar;
                                varScope.registerVariable(pVar);
                            }
                            f.setParameters(params);
                            Expression bodyExpr = translateExpression(node.getBody());
                            bodyExpr = new TreatExpression(currCtx, bodyExpr, sign.getReturnType());
                            popVariableScope();
                            f.setBody(bodyExpr);
                        }
                        break;
                    }
                }
            }
        }
    }

    private SequenceType createSequenceType(ASTNode type) throws SystemException {
        switch (type.getTag()) {
            case TYPE_DECLARATION: {
                TypeDeclNode tDecl = (TypeDeclNode) type;
                return createSequenceType(tDecl.getType());
            }

            case SEQUENCE_TYPE: {
                SequenceTypeNode sType = (SequenceTypeNode) type;

                if (sType.getItemType() == null) {
                    return SequenceType.create(AnyItemType.INSTANCE, Quantifier.QUANT_STAR);
                }

                TypeQuantifier tq = sType.getQuantifier();
                Quantifier q = Quantifier.QUANT_ONE;
                if (tq != null) {
                    switch (tq) {
                        case QUANT_QUESTION:
                            q = Quantifier.QUANT_QUESTION;
                            break;

                        case QUANT_PLUS:
                            q = Quantifier.QUANT_PLUS;
                            break;

                        case QUANT_STAR:
                            q = Quantifier.QUANT_STAR;
                            break;
                    }
                }

                ItemType iType = createItemType(sType.getItemType());
                return SequenceType.create(iType, q);
            }

            case EMPTY_SEQUENCE_TYPE: {
                return SequenceType.create(EmptySequenceType.INSTANCE, Quantifier.QUANT_ZERO);
            }

            case SINGLE_TYPE: {
                SingleTypeNode stNode = (SingleTypeNode) type;
                ItemType iType = createItemType(stNode.getAtomicType());
                return SequenceType.create(iType, stNode.isOptional() ? Quantifier.QUANT_QUESTION
                        : Quantifier.QUANT_ONE);
            }

            default:
                throw new IllegalStateException("Unknown node: " + type.getTag());
        }
    }

    private ItemType createItemType(ASTNode itemType) throws SystemException {
        switch (itemType.getTag()) {
            case ITEM_TYPE:
                return AnyItemType.INSTANCE;

            case ATOMIC_TYPE: {
                AtomicTypeNode atNode = (AtomicTypeNode) itemType;
                QName tName = createQName(atNode.getName());
                SchemaType sType = moduleCtx.lookupSchemaType(tName);
                if (sType == null || !sType.isAtomicType()) {
                    throw new SystemException(ErrorCode.XPST0051, atNode.getSourceLocation());
                }
                return (ItemType) sType;
            }

            case ANY_NODE_TEST:
                return AnyNodeType.INSTANCE;

            case DOCUMENT_TEST: {
                DocumentTestNode dt = (DocumentTestNode) itemType;
                if (dt.getElementTest() == null) {
                    return DocumentType.ANYDOCUMENT;
                }
                ElementType eType = (ElementType) createItemType(dt.getElementTest());
                return new DocumentType(eType);
            }

            case TEXT_TEST:
                return TextType.INSTANCE;

            case COMMENT_TEST:
                return CommentType.INSTANCE;

            case PI_TEST: {
                PITestNode pit = (PITestNode) itemType;
                if (pit.getTarget() == null) {
                    return ProcessingInstructionType.ANYPI;
                }
                return new ProcessingInstructionType(pit.getTarget());
            }

            case ATTRIBUTE_TEST: {
                AttributeTestNode at = (AttributeTestNode) itemType;
                if (at.getNameTest() == null) {
                    return AttributeType.ANYATTRIBUTE;
                }
                NameTestNode ntNode = at.getNameTest();
                NameTest nt = NameTest.STAR_NAMETEST;
                if (ntNode.getPrefix() == null && ntNode.getLocalName() == null) {
                    if (at.getTypeName() == null) {
                        return AttributeType.ANYATTRIBUTE;
                    }
                } else {
                    String uri;
                    if (!"".equals(ntNode.getPrefix())) {
                        uri = currCtx.lookupNamespaceUri(ntNode.getPrefix());
                        if (uri == null) {
                            throw new SystemException(ErrorCode.XPST0081, ntNode.getSourceLocation());
                        }
                    } else {
                        uri = "";
                    }
                    nt = new NameTest(uri, ntNode.getLocalName());
                }
                SchemaType cType = BuiltinTypeRegistry.XS_ANY_ATOMIC;
                if (at.getTypeName() != null) {
                    cType = moduleCtx.lookupSchemaType(createQName(at.getTypeName()));
                    if (cType == null) {
                        throw new SystemException(ErrorCode.XPST0051, at.getSourceLocation());
                    }
                }
                return new AttributeType(nt, cType);
            }

            case SCHEMA_ATTRIBUTE_TEST: {
                throw new UnsupportedOperationException();
            }

            case ELEMENT_TEST: {
                ElementTestNode et = (ElementTestNode) itemType;
                if (et.getNameTest() == null) {
                    return ElementType.ANYELEMENT;
                }
                NameTestNode ntNode = et.getNameTest();
                NameTest nt = NameTest.STAR_NAMETEST;
                if (ntNode.getPrefix() == null && ntNode.getLocalName() == null) {
                    if (et.getTypeName() == null) {
                        return ElementType.ANYELEMENT;
                    }
                } else {
                    String uri;
                    if (!"".equals(ntNode.getPrefix())) {
                        uri = currCtx.lookupNamespaceUri(ntNode.getPrefix());
                        if (uri == null) {
                            throw new SystemException(ErrorCode.XPST0081, ntNode.getSourceLocation());
                        }
                    } else {
                        uri = "";
                    }
                    nt = new NameTest(uri, ntNode.getLocalName());
                }
                SchemaType cType = AnyType.INSTANCE;
                if (et.getTypeName() != null) {
                    cType = moduleCtx.lookupSchemaType(createQName(et.getTypeName()));
                    if (cType == null) {
                        throw new SystemException(ErrorCode.XPST0051, et.getSourceLocation());
                    }
                }
                return new ElementType(nt, cType, et.isNillable());
            }

            case SCHEMA_ELEMENT_TEST: {
                throw new UnsupportedOperationException();
            }

            default:
                throw new IllegalStateException("Unknown node: " + itemType.getTag());
        }
    }

    private Expression translateExpression(ASTNode value) throws SystemException {
        final AtomicValueFactory avf = ccb.getAtomicValueFactory();
        switch (value.getTag()) {
            case EXPRESSION: {
                ExprNode node = (ExprNode) value;
                List<Expression> eList = translateExpressionList(node.getExpressions());
                if (eList.size() == 1) {
                    return eList.get(0);
                }
                return createConcatenation(eList);
            }

            case UNARY_EXPRESSION: {
                UnaryExprNode ueNode = (UnaryExprNode) value;
                boolean neg = false;
                for (UnaryExprNode.Sign s : ueNode.getSigns()) {
                    if (UnaryExprNode.Sign.MINUS.equals(s)) {
                        neg = !neg;
                    }
                }
                Expression e = translateExpression(ueNode.getExpr());
                if (neg) {
                    e = ExpressionBuilder.functionCall(currCtx, BuiltinOperators.NUMERIC_UNARY_MINUS, normalize(
                            currCtx, e, BuiltinOperators.NUMERIC_UNARY_MINUS.getSignature().getParameterType(0)));
                }
                return e;
            }

            case INFIX_EXPRESSION: {
                InfixExprNode ie = (InfixExprNode) value;
                Function operator = getOperator(ie.getOperator());
                Signature sign = operator.getSignature();
                Expression arg1 = normalize(currCtx, translateExpression(ie.getLeftExpr()), sign.getParameterType(0));
                Expression arg2 = normalize(currCtx, translateExpression(ie.getRightExpr()), sign.getParameterType(1));
                return ExpressionBuilder.functionCall(currCtx, operator, arg1, arg2);
            }

            case ENCLOSED_EXPRESSION: {
                EnclosedExprNode ee = (EnclosedExprNode) value;
                return translateExpression(ee.getExpression());
            }

            case PATH_EXPRESSION:
                return translatePathExpr((PathExprNode) value);

            case FUNCTION_EXPRESSION: {
                FunctionExprNode fnNode = (FunctionExprNode) value;
                List<Expression> args = new ArrayList<Expression>();
                for (ASTNode an : fnNode.getArguments()) {
                    args.add(translateExpression(an));
                }
                QName name = createQName(fnNode.getName());
                SchemaType type = moduleCtx.lookupSchemaType(name);
                if (type != null && args.size() < 2) {
                    if (!type.isAtomicType()) {
                        throw new SystemException(ErrorCode.XPST0051, fnNode.getName().getSourceLocation());
                    }
                    Expression arg = args.isEmpty() ? deflate(currCtx, new VariableReferenceExpression(currCtx,
                            varScope.lookupVariable(XMLQueryCompilerConstants.DOT_VAR_NAME))) : args.get(0);
                    return new CastExpression(currCtx, arg, SequenceType.create((ItemType) type,
                            Quantifier.QUANT_QUESTION));
                }
                QName fName = createQName(fnNode.getName(), moduleCtx.getDefaultFunctionNamespaceUri());
                if (BuiltinFunctions.FN_POSITION_QNAME.equals(fName)) {
                    Variable var = varScope.lookupVariable(XMLQueryCompilerConstants.POS_VAR_NAME);
                    return new VariableReferenceExpression(currCtx, var);
                }
                if (BuiltinFunctions.FN_LAST_QNAME.equals(fName)) {
                    Variable var = varScope.lookupVariable(XMLQueryCompilerConstants.LAST_VAR_NAME);
                    return new VariableReferenceExpression(currCtx, var);
                }
                int nArgs = fnNode.getArguments().size();
                Function fn = moduleCtx.lookupFunction(fName, nArgs);
                if (fn != null && fn.useContextImplicitly()) {
                    args.add(deflate(currCtx, new VariableReferenceExpression(currCtx, varScope
                            .lookupVariable(XMLQueryCompilerConstants.DOT_VAR_NAME))));
                    nArgs = fnNode.getArguments().size();
                    fn = moduleCtx.lookupFunction(fName, nArgs);
                }
                if (fn == null) {
                    Function[] fns = moduleCtx.lookupFunctions(fName);
                    if (fns != null) {
                        for (int i = 0; i < fns.length && i <= nArgs; ++i) {
                            if (fns[i] != null && fns[i].getSignature().isVarArgs()) {
                                fn = fns[i];
                                break;
                            }
                        }
                    }
                }
                if (fn == null) {
                    throw new SystemException(ErrorCode.XPST0017, fnNode.getName().getSourceLocation());
                }
                Signature sign = fn.getSignature();
                for (int i = 0; i < args.size(); ++i) {
                    SequenceType argType = sign.getParameterType(i);
                    args.set(i, normalize(currCtx, args.get(i), argType));
                }
                return new FunctionCallExpression(currCtx, fn, args);
            }

            case TYPE_EXPRESSION: {
                TypeExprNode teNode = (TypeExprNode) value;
                Expression input = translateExpression(teNode.getExpr());
                SequenceType type = createSequenceType(teNode.getType());
                switch (teNode.getOperator()) {
                    case CAST:
                        return new CastExpression(currCtx, input, type);

                    case CASTABLE:
                        return new CastableExpression(currCtx, input, type);

                    case INSTANCEOF:
                        return new InstanceofExpression(currCtx, input, type);

                    case TREAT:
                        return new TreatExpression(currCtx, input, type);

                    default:
                        throw new IllegalStateException("Unknown type operator: " + teNode.getOperator());
                }
            }

            case EXTENSION_EXPRESSION: {
                ExtensionExprNode eNode = (ExtensionExprNode) value;
                if (eNode.getExpr() == null) {
                    throw new SystemException(ErrorCode.XQST0079, eNode.getSourceLocation());
                }
                return translateExpression(eNode.getExpr());
            }

            case PARENTHESIZED_EXPRESSION: {
                ParenthesizedExprNode peNode = (ParenthesizedExprNode) value;
                ASTNode eNode = peNode.getExpr();
                if (eNode == null) {
                    return createConcatenation(Collections.<Expression> emptyList());
                }
                return translateExpression(((ParenthesizedExprNode) value).getExpr());
            }

            case LITERAL: {
                LiteralNode lNode = (LiteralNode) value;
                String image = lNode.getImage();
                LiteralNode.LiteralType lType = lNode.getType();
                SequenceType t = null;
                Object v;
                switch (lType) {
                    case DECIMAL:
                        t = SequenceType.create(BuiltinTypeRegistry.XS_DECIMAL, Quantifier.QUANT_ONE);
                        v = avf.createDecimal(new BigDecimal(image));
                        break;
                    case DOUBLE:
                        t = SequenceType.create(BuiltinTypeRegistry.XS_DOUBLE, Quantifier.QUANT_ONE);
                        v = avf.createDouble(Double.parseDouble(image));
                        break;
                    case INTEGER:
                        t = SequenceType.create(BuiltinTypeRegistry.XS_INTEGER, Quantifier.QUANT_ONE);
                        v = avf.createInteger(new BigInteger(image));
                        break;
                    case STRING:
                        t = SequenceType.create(BuiltinTypeRegistry.XS_STRING, Quantifier.QUANT_ONE);
                        image = unquote(image);
                        v = avf.createString(image);
                        break;
                    default:
                        throw new IllegalStateException("Unknown type: " + lType);
                }
                return new ConstantExpression(currCtx, v, t);
            }

            case DIRECT_PI_CONSTRUCTOR: {
                DirectPIConstructorNode dpicNode = (DirectPIConstructorNode) value;
                String target = dpicNode.getTarget();
                String content = dpicNode.getContent();
                return new PINodeConstructorExpression(currCtx,
                        new ConstantExpression(currCtx, avf.createString(target), SequenceType.create(
                                BuiltinTypeRegistry.XS_STRING, Quantifier.QUANT_ONE)), new ConstantExpression(currCtx,
                                avf.createString(content), SequenceType.create(BuiltinTypeRegistry.XS_STRING,
                                        Quantifier.QUANT_ONE)));
            }

            case DIRECT_COMMENT_CONSTRUCTOR:
                return new CommentNodeConstructorExpression(currCtx, new ConstantExpression(currCtx, avf
                        .createString(((DirectCommentConstructorNode) value).getContent()), SequenceType.create(
                        BuiltinTypeRegistry.XS_STRING, Quantifier.QUANT_ONE)));

            case DIRECT_ELEMENT_CONSTRUCTOR: {
                DirectElementConstructorNode decNode = (DirectElementConstructorNode) value;
                QNameNode startName = decNode.getStartTagName();
                QNameNode endName = decNode.getEndTagName();
                if (endName != null
                        && (!startName.getPrefix().equals(endName.getPrefix()) || !startName.getLocalName().equals(
                                endName.getLocalName()))) {
                    throw new SystemException(ErrorCode.XPST0003, endName.getSourceLocation());
                }
                pushContext();
                for (DirectAttributeConstructorNode acNode : decNode.getAttributes()) {
                    QNameNode aName = acNode.getName();
                    if ("xmlns".equals(aName.getPrefix())) {
                        List<ASTNode> values = acNode.getValue();
                        if (values.size() != 1 || !ASTTag.CONTENT_CHARS.equals(values.get(0).getTag())) {
                            throw new SystemException(ErrorCode.XQST0022, acNode.getSourceLocation());
                        }

                        currCtx.registerNamespaceUri(aName.getLocalName(), unquote(((ContentCharsNode) values.get(0))
                                .getContent()));
                    }
                }
                List<Expression> content = new ArrayList<Expression>();
                for (DirectAttributeConstructorNode acNode : decNode.getAttributes()) {
                    QNameNode aName = acNode.getName();
                    if (!"xmlns".equals(aName.getPrefix())) {
                        content.add(translateExpression(acNode));
                    }
                }
                Expression name = new ConstantExpression(currCtx, avf.createQName(createQName(startName, moduleCtx
                        .getDefaultElementNamespaceUri())), SequenceType.create(BuiltinTypeRegistry.XS_QNAME,
                        Quantifier.QUANT_ONE));
                for (ASTNode cVal : decNode.getContent()) {
                    switch (cVal.getTag()) {
                        case CONTENT_CHARS:
                            content.add(new TextNodeConstructorExpression(currCtx, new ConstantExpression(currCtx, avf
                                    .createUntypedAtomic(((ContentCharsNode) cVal).getContent()), SequenceType.create(
                                    BuiltinTypeRegistry.XS_UNTYPED_ATOMIC, Quantifier.QUANT_ONE))));
                            break;

                        default:
                            content.add(translateExpression(cVal));
                    }
                }
                Expression contentExpr = content.size() == 1 ? content.get(0) : createConcatenation(content);
                Expression result = new ElementNodeConstructorExpression(currCtx, name, contentExpr);
                popContext();
                return result;
            }

            case DIRECT_ATTRIBUTE_CONSTRUCTOR: {
                DirectAttributeConstructorNode dacNode = (DirectAttributeConstructorNode) value;
                QName aQName = createQName(dacNode.getName());
                List<Expression> attrContent = new ArrayList<Expression>();
                for (ASTNode aVal : dacNode.getValue()) {
                    switch (aVal.getTag()) {
                        case CONTENT_CHARS:
                            attrContent.add(new ConstantExpression(currCtx, avf
                                    .createUntypedAtomic(((ContentCharsNode) aVal).getContent()), SequenceType.create(
                                    BuiltinTypeRegistry.XS_UNTYPED_ATOMIC, Quantifier.QUANT_ONE)));
                            break;

                        default:
                            attrContent.add(translateExpression(aVal));
                    }
                }
                Expression content = attrContent.size() == 1 ? attrContent.get(0) : createConcatenation(attrContent);
                return new AttributeNodeConstructorExpression(currCtx, new ConstantExpression(currCtx, avf
                        .createQName(aQName), SequenceType.create(BuiltinTypeRegistry.XS_QNAME, Quantifier.QUANT_ONE)),
                        content);
            }

            case CONTEXT_ITEM:
                return new VariableReferenceExpression(currCtx, varScope
                        .lookupVariable(XMLQueryCompilerConstants.DOT_VAR_NAME));

            case IF_EXPRESSION: {
                IfExprNode ieNode = (IfExprNode) value;
                Expression cond = translateExpression(ieNode.getIfExpr());
                cond = ExpressionBuilder.functionCall(currCtx, BuiltinFunctions.FN_BOOLEAN_1, cond);
                Expression tExpr = translateExpression(ieNode.getThenExpr());
                Expression eExpr = translateExpression(ieNode.getElseExpr());
                return new IfThenElseExpression(currCtx, cond, tExpr, eExpr);
            }

            case VARIABLE_REFERENCE: {
                VarRefNode vrNode = (VarRefNode) value;
                QName vName = createQName(vrNode.getVariable());
                Variable var = varScope.lookupVariable(vName);
                if (var == null) {
                    throw new SystemException(ErrorCode.XPST0008, vrNode.getSourceLocation());
                }
                return new TreatExpression(currCtx, deflate(currCtx, new VariableReferenceExpression(currCtx, var)),
                        var.getDeclaredStaticType());
            }

            case FLWOR_EXPRESSION: {
                FLWORExprNode fNode = (FLWORExprNode) value;
                List<FLWORClauseNode> cNodes = fNode.getClauses();
                List<FLWORExpression.Clause> clauses = new ArrayList<FLWORExpression.Clause>();
                int pushCount = 0;
                for (FLWORClauseNode cNode : cNodes) {
                    switch (cNode.getTag()) {
                        case FOR_CLAUSE: {
                            ForClauseNode fcNode = (ForClauseNode) cNode;
                            for (ForVarDeclNode fvdNode : fcNode.getVariables()) {
                                Expression seq = translateExpression(fvdNode.getSequence());
                                pushVariableScope();
                                ForLetVariable forVar = new ForLetVariable(VarTag.FOR,
                                        createQName(fvdNode.getForVar()), seq);
                                SequenceType forVarType = SequenceType.create(AnyItemType.INSTANCE,
                                        Quantifier.QUANT_ONE);
                                if (fvdNode.getType() != null) {
                                    forVarType = createSequenceType(fvdNode.getType());
                                }
                                forVar.setDeclaredStaticType(forVarType);
                                varScope.registerVariable(forVar);
                                PositionVariable posVar = null;
                                if (fvdNode.getPosVar() != null) {
                                    posVar = new PositionVariable(createQName(fvdNode.getPosVar()));
                                    posVar.setDeclaredStaticType(SequenceType.create(BuiltinTypeRegistry.XS_INTEGER,
                                            Quantifier.QUANT_ONE));
                                    varScope.registerVariable(posVar);
                                }
                                ScoreVariable scoreVar = null;
                                if (fvdNode.getScoreVar() != null) {
                                    scoreVar = new ScoreVariable(createQName(fvdNode.getScoreVar()));
                                    scoreVar.setDeclaredStaticType(SequenceType.create(BuiltinTypeRegistry.XS_DOUBLE,
                                            Quantifier.QUANT_ONE));
                                    varScope.registerVariable(scoreVar);
                                }
                                clauses.add(new FLWORExpression.ForClause(forVar, posVar, scoreVar));
                                ++pushCount;
                            }
                            break;
                        }
                        case LET_CLAUSE: {
                            LetClauseNode lcNode = (LetClauseNode) cNode;
                            for (LetVarDeclNode lvdNode : lcNode.getVariables()) {
                                Expression seq = translateExpression(lvdNode.getSequence());
                                pushVariableScope();
                                ForLetVariable letVar = new ForLetVariable(VarTag.LET,
                                        createQName(lvdNode.getLetVar()), seq);
                                SequenceType letVarType = SequenceType.create(AnyItemType.INSTANCE,
                                        Quantifier.QUANT_ONE);
                                if (lvdNode.getType() != null) {
                                    letVarType = createSequenceType(lvdNode.getType());
                                }
                                letVar.setDeclaredStaticType(letVarType);
                                varScope.registerVariable(letVar);
                                clauses.add(new FLWORExpression.LetClause(letVar));
                                ++pushCount;
                            }
                            break;
                        }
                        case WHERE_CLAUSE: {
                            WhereClauseNode wcNode = (WhereClauseNode) cNode;
                            Expression condExpr = translateExpression(wcNode.getCondition());
                            condExpr = ExpressionBuilder.functionCall(currCtx, BuiltinFunctions.FN_BOOLEAN_1, condExpr);
                            clauses.add(new FLWORExpression.WhereClause(condExpr));
                            break;
                        }
                        case ORDERBY_CLAUSE: {
                            OrderbyClauseNode ocNode = (OrderbyClauseNode) cNode;
                            List<Expression> oExprs = new ArrayList<Expression>();
                            List<FLWORExpression.OrderDirection> oDirs = new ArrayList<FLWORExpression.OrderDirection>();
                            List<FLWORExpression.EmptyOrder> eOrders = new ArrayList<FLWORExpression.EmptyOrder>();
                            List<String> collations = new ArrayList<String>();
                            for (OrderSpecNode osNode : ocNode.getOrderSpec()) {
                                oExprs.add(translateExpression(osNode.getExpression()));
                                XQueryConstants.OrderDirection oDir = osNode.getDirection();
                                if (oDir != null) {
                                    switch (oDir) {
                                        case ASCENDING:
                                            oDirs.add(FLWORExpression.OrderDirection.ASCENDING);
                                            break;
                                        case DESCENDING:
                                            oDirs.add(FLWORExpression.OrderDirection.DESCENDING);
                                            break;
                                    }
                                } else {
                                    oDirs.add(FLWORExpression.OrderDirection.ASCENDING);
                                }
                                StaticContext.EmptyOrderProperty eoProp = osNode.getEmptyOrder();
                                if (eoProp != null) {
                                    switch (osNode.getEmptyOrder()) {
                                        case GREATEST:
                                            eOrders.add(FLWORExpression.EmptyOrder.GREATEST);
                                            break;
                                        case LEAST:
                                            eOrders.add(FLWORExpression.EmptyOrder.LEAST);
                                            break;
                                    }
                                } else {
                                    eOrders.add(FLWORExpression.EmptyOrder.DEFAULT);
                                }
                                collations.add(osNode.getCollation());
                            }
                            clauses.add(new FLWORExpression.OrderbyClause(oExprs, oDirs, eOrders, collations, ocNode
                                    .isStable()));
                            break;
                        }
                        default:
                            throw new IllegalStateException("Unknown clause: " + cNode.getTag());
                    }
                }
                Expression rExpr = translateExpression(fNode.getReturnExpr());
                for (int i = 0; i < pushCount; ++i) {
                    popVariableScope();
                }
                return new FLWORExpression(currCtx, clauses, rExpr);
            }

            case QUANTIFIED_EXPRESSION: {
                QuantifiedExprNode qeNode = (QuantifiedExprNode) value;
                List<ForLetVariable> vars = new ArrayList<ForLetVariable>();
                int pushCount = 0;
                for (QuantifiedVarDeclNode qvdNode : qeNode.getVariables()) {
                    Expression seq = translateExpression(qvdNode.getSequence());
                    pushVariableScope();
                    ForLetVariable var = new ForLetVariable(VarTag.FOR, createQName(qvdNode.getVariable()), seq);
                    SequenceType varType = SequenceType.create(AnyItemType.INSTANCE, Quantifier.QUANT_ONE);
                    if (qvdNode.getType() != null) {
                        varType = createSequenceType(qvdNode.getType());
                    }
                    var.setDeclaredStaticType(varType);
                    vars.add(var);
                    varScope.registerVariable(var);
                    ++pushCount;
                }
                Expression sExpr = translateExpression(qeNode.getSatisfiesExpr());
                for (int i = 0; i < pushCount; ++i) {
                    popVariableScope();
                }
                return new QuantifiedExpression(currCtx, QuantifiedExprNode.QuantifierType.SOME.equals(qeNode
                        .getQuant()) ? QuantifiedExpression.Quantification.SOME
                        : QuantifiedExpression.Quantification.EVERY, vars, sExpr);
            }

            case TYPESWITCH_EXPRESSION: {
                TypeswitchExprNode teNode = (TypeswitchExprNode) value;
                Expression sExpr = translateExpression(teNode.getSwitchExpr());
                ForLetVariable tVar = new ForLetVariable(VarTag.LET, createVarName(), sExpr);
                tVar.setDeclaredStaticType(SequenceType.create(AnyItemType.INSTANCE, Quantifier.QUANT_STAR));
                pushVariableScope();
                varScope.registerVariable(tVar);
                List<TypeswitchExpression.Case> cases = new ArrayList<TypeswitchExpression.Case>();
                for (CaseClauseNode ccNode : teNode.getCaseClauses()) {
                    SequenceType type = createSequenceType(ccNode.getType());
                    ForLetVariable caseVar = null;
                    pushVariableScope();
                    if (ccNode.getCaseVar() != null) {
                        caseVar = new ForLetVariable(VarTag.LET, createQName(ccNode.getCaseVar()), new TreatExpression(
                                currCtx, new VariableReferenceExpression(currCtx, tVar), type));
                        caseVar.setDeclaredStaticType(type);
                        varScope.registerVariable(caseVar);
                    }
                    Expression cExpr = translateExpression(ccNode.getValueExpr());
                    TypeswitchExpression.Case c = new TypeswitchExpression.Case(caseVar, type, cExpr);
                    cases.add(c);
                    popVariableScope();
                }
                Expression dExpr = translateExpression(teNode.getDefaultClause());
                popVariableScope();
                return new TypeswitchExpression(currCtx, tVar, cases, dExpr);
            }

            case COMPUTED_TEXT_CONSTRUCTOR: {
                ComputedTextConstructorNode cNode = (ComputedTextConstructorNode) value;
                Expression content = cNode.getContent() == null ? ExpressionBuilder.functionCall(currCtx,
                        BuiltinOperators.CONCATENATE) : translateExpression(cNode.getContent());
                return new TextNodeConstructorExpression(currCtx, content);
            }

            case COMPUTED_PI_CONSTRUCTOR: {
                ComputedPIConstructorNode cNode = (ComputedPIConstructorNode) value;
                Expression content = cNode.getContent() == null ? ExpressionBuilder.functionCall(currCtx,
                        BuiltinOperators.CONCATENATE) : translateExpression(cNode.getContent());
                return new PINodeConstructorExpression(currCtx, translateExpression(cNode.getTarget()), content);
            }

            case COMPUTED_COMMENT_CONSTRUCTOR: {
                ComputedCommentConstructorNode cNode = (ComputedCommentConstructorNode) value;
                Expression content = cNode.getContent() == null ? ExpressionBuilder.functionCall(currCtx,
                        BuiltinOperators.CONCATENATE) : translateExpression(cNode.getContent());
                return new CommentNodeConstructorExpression(currCtx, content);
            }

            case COMPUTED_DOCUMENT_CONSTRUCTOR:
                return new DocumentNodeConstructorExpression(currCtx,
                        translateExpression(((ComputedDocumentConstructorNode) value).getContent()));

            case COMPUTED_ELEMENT_CONSTRUCTOR: {
                ComputedElementConstructorNode cNode = (ComputedElementConstructorNode) value;
                Expression eName = new CastExpression(currCtx, translateExpression(cNode.getName()), SequenceType
                        .create(BuiltinTypeRegistry.XS_QNAME, Quantifier.QUANT_ONE));
                Expression content = cNode.getContent() == null ? ExpressionBuilder.functionCall(currCtx,
                        BuiltinOperators.CONCATENATE) : translateExpression(cNode.getContent());
                return new ElementNodeConstructorExpression(currCtx, eName, content);
            }

            case COMPUTED_ATTRIBUTE_CONSTRUCTOR: {
                ComputedAttributeConstructorNode cNode = (ComputedAttributeConstructorNode) value;
                Expression aName = new CastExpression(currCtx, translateExpression(cNode.getName()), SequenceType
                        .create(BuiltinTypeRegistry.XS_QNAME, Quantifier.QUANT_ONE));
                Expression content = cNode.getContent() == null ? ExpressionBuilder.functionCall(currCtx,
                        BuiltinOperators.CONCATENATE) : translateExpression(cNode.getContent());
                return new AttributeNodeConstructorExpression(currCtx, aName, content);
            }

            case QNAME:
                return new ConstantExpression(currCtx, avf.createQName(createQName((QNameNode) value)), SequenceType
                        .create(BuiltinTypeRegistry.XS_QNAME, Quantifier.QUANT_ONE));

            case NCNAME:
                return new ConstantExpression(currCtx, avf.createString(((NCNameNode) value).getName()), SequenceType
                        .create(BuiltinTypeRegistry.XS_STRING, Quantifier.QUANT_ONE));

            case CDATA_SECTION:
                return new TextNodeConstructorExpression(currCtx, new ConstantExpression(currCtx, avf
                        .createString(((CDataSectionNode) value).getContent()), SequenceType.create(
                        BuiltinTypeRegistry.XS_STRING, Quantifier.QUANT_ONE)));

            case ORDERED_EXPRESSION:
                return ExpressionBuilder.functionCall(currCtx, BuiltinOperators.ORDERED,
                        translateExpression(((OrderedExprNode) value).getExpr()));

            case UNORDERED_EXPRESSION:
                return ExpressionBuilder.functionCall(currCtx, BuiltinOperators.UNORDERED,
                        translateExpression(((UnorderedExprNode) value).getExpr()));

            case VALIDATE_EXPRESSION: {
                ValidateExprNode vNode = (ValidateExprNode) value;
                ValidateExpression.Mode mode = ValidateExpression.Mode.DEFAULT;
                if (vNode.getMode() != null) {
                    mode = XQueryConstants.ValidationMode.LAX.equals(vNode.getMode()) ? ValidateExpression.Mode.LAX
                            : ValidateExpression.Mode.STRICT;
                }
                return new ValidateExpression(currCtx, translateExpression(vNode.getExpr()), mode);
            }

            default:
                throw new IllegalStateException("Unknown node: " + value.getTag());
        }
    }

    private String unquote(String image) {
        if (image.startsWith("'")) {
            image = image.substring(1, image.length() - 1).replaceAll("''", "'");
        } else {
            image = image.substring(1, image.length() - 1).replaceAll("\"\"", "\"");
        }
        return image;
    }

    private static Expression deflate(StaticContext ctx, Expression expr) {
        return ExpressionBuilder.functionCall(ctx, BuiltinOperators.DEFLATE_SEQUENCES, expr);
    }

    private static Expression normalize(StaticContext ctx, Expression e, SequenceType type) {
        if (type.getItemType().isAtomicType()) {
            Expression atomizedExpr = atomize(ctx, e);
            AtomicType aType = (AtomicType) type.getItemType();
            if (TypeUtils.isSubtypeTypeOf(aType, BuiltinTypeRegistry.XS_BOOLEAN)) {
                return ExpressionBuilder.functionCall(ctx, BuiltinFunctions.FN_BOOLEAN_1, atomizedExpr);
            }
            return promote(ctx, atomize(ctx, e), type);
        } else {
            return new TreatExpression(ctx, e, type);
        }
    }

    private static Expression promote(StaticContext ctx, Expression e, SequenceType type) {
        return new PromoteExpression(ctx, e, type);
    }

    private static Expression atomize(StaticContext ctx, Expression e) {
        return ExpressionBuilder.functionCall(ctx, BuiltinFunctions.FN_DATA_1, e);
    }

    private Expression translatePathExpr(PathExprNode pe) throws SystemException {
        Expression ctxExpr = null;

        PathType type = pe.getPathType();
        if (type != null) {
            Variable dotVar = varScope.lookupVariable(XMLQueryCompilerConstants.DOT_VAR_NAME);
            List<Expression> args = new ArrayList<Expression>();
            args.add(new VariableReferenceExpression(currCtx, dotVar));
            Expression rootExpr = new FunctionCallExpression(currCtx, BuiltinFunctions.FN_ROOT_1, args);
            if (PathType.SLASH.equals(type)) {
                ctxExpr = rootExpr;
            } else {
                ctxExpr = new PathStepExpression(currCtx, treatAsNode(currCtx, rootExpr), AxisKind.DESCENDANT_OR_SELF,
                        AnyNodeType.INSTANCE);
            }
        }

        if (pe.getPaths() != null) {
            for (RelativePathExprNode rpen : pe.getPaths()) {
                if (PathType.SLASH_SLASH.equals(rpen.getPathType())) {
                    ctxExpr = new PathStepExpression(currCtx, treatAsNode(currCtx, ctxExpr),
                            AxisKind.DESCENDANT_OR_SELF, AnyNodeType.INSTANCE);
                }
                FLWORExpression flwor = null;
                if (ctxExpr != null) {
                    pushVariableScope();
                    flwor = createWrappingFLWOR(ctxExpr);
                    ctxExpr = null;
                }

                List<ASTNode> predicates = null;

                ASTNode pathNode = rpen.getPath();
                boolean fwdPath = true;
                if (ASTTag.AXIS_STEP.equals(pathNode.getTag())) {
                    AxisStepNode axisNode = (AxisStepNode) pathNode;
                    predicates = axisNode.getPredicates();
                    AxisStepNode.Axis axis = axisNode.getAxis();
                    AxisKind axisKind;
                    boolean attribute = false;
                    switch (axis) {
                        case ABBREV:
                        case CHILD:
                            axisKind = AxisKind.CHILD;
                            break;

                        case ABBREV_ATTRIBUTE:
                        case ATTRIBUTE:
                            axisKind = AxisKind.ATTRIBUTE;
                            attribute = true;
                            break;

                        case ANCESTOR:
                            axisKind = AxisKind.ANCESTOR;
                            fwdPath = false;
                            break;

                        case ANCESTOR_OR_SELF:
                            axisKind = AxisKind.ANCESTOR_OR_SELF;
                            fwdPath = false;
                            break;

                        case DESCENDANT:
                            axisKind = AxisKind.DESCENDANT;
                            break;

                        case DESCENDANT_OR_SELF:
                            axisKind = AxisKind.DESCENDANT_OR_SELF;
                            break;

                        case DOT_DOT:
                        case PARENT:
                            axisKind = AxisKind.PARENT;
                            fwdPath = false;
                            break;

                        case FOLLOWING:
                            axisKind = AxisKind.FOLLOWING;
                            break;

                        case FOLLOWING_SIBLING:
                            axisKind = AxisKind.FOLLOWING_SIBLING;
                            break;

                        case PRECEDING:
                            axisKind = AxisKind.PRECEDING;
                            fwdPath = false;
                            break;

                        case PRECEDING_SIBLING:
                            axisKind = AxisKind.PRECEDING_SIBLING;
                            fwdPath = false;
                            break;

                        case SELF:
                            axisKind = AxisKind.SELF;
                            break;

                        default:
                            throw new IllegalStateException("Unknown axis: " + axis);
                    }
                    if (ctxExpr == null) {
                        ctxExpr = new VariableReferenceExpression(currCtx, varScope
                                .lookupVariable(XMLQueryCompilerConstants.DOT_VAR_NAME));
                    }

                    ASTNode nodeTest = axisNode.getNodeTest();
                    NodeType nt = AnyNodeType.INSTANCE;
                    if (nodeTest != null) {
                        switch (nodeTest.getTag()) {
                            case NAME_TEST: {
                                NameTestNode ntn = (NameTestNode) nodeTest;
                                String uri;
                                if (!"".equals(ntn.getPrefix())) {
                                    uri = currCtx.lookupNamespaceUri(ntn.getPrefix());
                                    if (uri == null) {
                                        throw new SystemException(ErrorCode.XPST0081, ntn.getSourceLocation());
                                    }
                                } else {
                                    uri = "";
                                }
                                NameTest nameTest = new NameTest(uri, ntn.getLocalName());
                                if (attribute) {
                                    nt = new AttributeType(nameTest, BuiltinTypeRegistry.XS_ANY_ATOMIC);
                                } else {
                                    nt = new ElementType(nameTest, AnyType.INSTANCE, true);
                                }
                                break;
                            }

                            case ANY_NODE_TEST:
                            case DOCUMENT_TEST:
                            case TEXT_TEST:
                            case COMMENT_TEST:
                            case PI_TEST:
                            case ATTRIBUTE_TEST:
                            case SCHEMA_ATTRIBUTE_TEST:
                            case ELEMENT_TEST:
                            case SCHEMA_ELEMENT_TEST:
                                nt = (NodeType) createItemType(nodeTest);
                                break;

                            default:
                                throw new IllegalStateException("Unknown node: " + nodeTest.getTag());
                        }
                    }
                    ctxExpr = new PathStepExpression(currCtx, treatAsNode(currCtx, ctxExpr), axisKind, nt);
                    ctxExpr = wrapSortAndDistinctNodes(ctxExpr, fwdPath);
                } else if (ASTTag.FILTER_EXPRESSION.equals(pathNode.getTag())) {
                    FilterExprNode filterNode = (FilterExprNode) pathNode;
                    predicates = filterNode.getPredicates();
                    ctxExpr = translateExpression(filterNode.getExpr());
                } else {
                    throw new IllegalStateException("Unknown path node: " + pathNode.getTag());
                }
                if (predicates != null && !predicates.isEmpty()) {
                    for (ASTNode pn : predicates) {
                        pushVariableScope();
                        FLWORExpression innerFLWOR = createWrappingFLWOR(ctxExpr);
                        List<FLWORExpression.Clause> clauses = innerFLWOR.getClauses();
                        Expression pExpr = translateExpression(pn);

                        ForLetVariable pVar = new ForLetVariable(VarTag.LET, createVarName(), pExpr);
                        FLWORExpression.LetClause pLC = new FLWORExpression.LetClause(pVar);
                        varScope.registerVariable(pVar);
                        clauses.add(pLC);

                        Expression typeTest = ExpressionBuilder.instanceOf(currCtx, new VariableReferenceExpression(
                                currCtx, pVar), SequenceType.create(BuiltinTypeRegistry.XSEXT_NUMERIC,
                                Quantifier.QUANT_ONE));
                        Expression posTest = ExpressionBuilder.functionCall(currCtx, BuiltinOperators.VALUE_EQ,
                                deflate(currCtx, new VariableReferenceExpression(currCtx, pVar)),
                                new VariableReferenceExpression(currCtx, varScope
                                        .lookupVariable(XMLQueryCompilerConstants.POS_VAR_NAME)));
                        Expression boolTest = ExpressionBuilder.functionCall(currCtx, BuiltinFunctions.FN_BOOLEAN_1,
                                deflate(currCtx, new VariableReferenceExpression(currCtx, pVar)));

                        clauses.add(new FLWORExpression.WhereClause(new IfThenElseExpression(currCtx, typeTest,
                                posTest, boolTest)));

                        innerFLWOR.setReturnExpression(new VariableReferenceExpression(currCtx, varScope
                                .lookupVariable(XMLQueryCompilerConstants.DOT_VAR_NAME)));
                        ctxExpr = innerFLWOR;

                        popVariableScope();
                    }
                }
                if (flwor != null) {
                    popVariableScope();
                    flwor.setReturnExpression(ctxExpr);
                    ctxExpr = flwor;
                }
            }
        }
        return ctxExpr;
    }

    private static Expression treatAsNode(StaticContext ctx, Expression expr) {
        return new TreatExpression(ctx, expr, SequenceType.create(AnyNodeType.INSTANCE, Quantifier.QUANT_STAR));
    }

    private Expression wrapSortAndDistinctNodes(Expression ctxExpr, boolean asc) {
        List<Expression> args = new ArrayList<Expression>();
        args.add(ctxExpr);
        return new FunctionCallExpression(currCtx, asc ? BuiltinOperators.SORT_DISTINCT_NODES_ASC
                : BuiltinOperators.SORT_DISTINCT_NODES_DESC, args);
    }

    private FLWORExpression createWrappingFLWOR(Expression seq) {
        List<FLWORExpression.Clause> clauses = new ArrayList<FLWORExpression.Clause>();

        ForLetVariable seqVar = new ForLetVariable(VarTag.LET, createVarName(), seq);
        FLWORExpression.LetClause seqLC = new FLWORExpression.LetClause(seqVar);
        varScope.registerVariable(seqVar);
        clauses.add(seqLC);

        List<Expression> cArgs = new ArrayList<Expression>();
        cArgs.add(deflate(currCtx, new VariableReferenceExpression(currCtx, seqVar)));
        ForLetVariable cVar = new ForLetVariable(VarTag.LET, XMLQueryCompilerConstants.LAST_VAR_NAME,
                new FunctionCallExpression(currCtx, BuiltinFunctions.FN_COUNT_1, cArgs));
        FLWORExpression.LetClause cLC = new FLWORExpression.LetClause(cVar);
        varScope.registerVariable(cVar);
        clauses.add(cLC);

        ForLetVariable dotVar = new ForLetVariable(VarTag.FOR, XMLQueryCompilerConstants.DOT_VAR_NAME, deflate(currCtx,
                new VariableReferenceExpression(currCtx, seqVar)));
        PositionVariable posVar = new PositionVariable(XMLQueryCompilerConstants.POS_VAR_NAME);
        FLWORExpression.ForClause dotFC = new FLWORExpression.ForClause(dotVar, posVar, null);
        varScope.registerVariable(dotVar);
        varScope.registerVariable(posVar);
        clauses.add(dotFC);

        return new FLWORExpression(currCtx, clauses, null);
    }

    private QName createVarName() {
        return new QName("$" + (varCounter++));
    }

    private Function getOperator(InfixOperator operator) {
        switch (operator) {
            case AND:
                return BuiltinOperators.AND;

            case DIV:
                return BuiltinOperators.DIVIDE;

            case EXCEPT:
                return BuiltinOperators.EXCEPT;

            case FOLLOWS:
                return BuiltinOperators.FOLLOWS;

            case GENERAL_EQ:
                return BuiltinOperators.GENERAL_EQ;

            case GENERAL_GE:
                return BuiltinOperators.GENERAL_GE;

            case GENERAL_GT:
                return BuiltinOperators.GENERAL_GT;

            case GENERAL_LE:
                return BuiltinOperators.GENERAL_LE;

            case GENERAL_LT:
                return BuiltinOperators.GENERAL_LT;

            case GENERAL_NE:
                return BuiltinOperators.GENERAL_NE;

            case IDIV:
                return BuiltinOperators.IDIV;

            case INTERSECT:
                return BuiltinOperators.INTERSECT;

            case IS:
                return BuiltinOperators.IS_SAME_NODE;

            case MINUS:
                return BuiltinOperators.SUBTRACT;

            case MOD:
                return BuiltinOperators.MOD;

            case MULTIPLY:
                return BuiltinOperators.MULTIPLY;

            case OR:
                return BuiltinOperators.OR;

            case PLUS:
                return BuiltinOperators.ADD;

            case PRECEDES:
                return BuiltinOperators.PRECEDES;

            case RANGE:
                return BuiltinOperators.TO;

            case UNION:
                return BuiltinOperators.UNION;

            case VALUE_EQ:
                return BuiltinOperators.VALUE_EQ;

            case VALUE_GE:
                return BuiltinOperators.VALUE_GE;

            case VALUE_GT:
                return BuiltinOperators.VALUE_GT;

            case VALUE_LE:
                return BuiltinOperators.VALUE_LE;

            case VALUE_LT:
                return BuiltinOperators.VALUE_LT;

            case VALUE_NE:
                return BuiltinOperators.VALUE_NE;
        }
        throw new IllegalStateException("Unknown operator: " + operator);
    }

    private Expression createConcatenation(List<Expression> list) {
        return new FunctionCallExpression(currCtx, BuiltinOperators.CONCATENATE, list);
    }

    private List<Expression> translateExpressionList(List<ASTNode> exprs) throws SystemException {
        List<Expression> result = new ArrayList<Expression>();
        for (ASTNode e : exprs) {
            result.add(translateExpression(e));
        }
        return result;
    }

    private interface VariableScope {
        public VariableScope getParentScope();

        public Variable lookupVariable(QName name);

        public void registerVariable(Variable var);
    }
}