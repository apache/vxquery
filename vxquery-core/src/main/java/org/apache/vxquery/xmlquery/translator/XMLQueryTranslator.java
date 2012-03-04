package org.apache.vxquery.xmlquery.translator;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import javax.xml.namespace.QName;

import org.apache.commons.lang3.mutable.Mutable;
import org.apache.commons.lang3.mutable.MutableObject;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.vxquery.compiler.CompilerControlBlock;
import org.apache.vxquery.compiler.algebricks.ConstantValue;
import org.apache.vxquery.context.StaticContext;
import org.apache.vxquery.context.StaticContextImpl;
import org.apache.vxquery.context.ThinStaticContextImpl;
import org.apache.vxquery.context.XQueryVariable;
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
import org.apache.vxquery.xmlquery.ast.ASTNode;
import org.apache.vxquery.xmlquery.ast.ASTTag;
import org.apache.vxquery.xmlquery.ast.AtomicTypeNode;
import org.apache.vxquery.xmlquery.ast.AttributeTestNode;
import org.apache.vxquery.xmlquery.ast.AxisStepNode;
import org.apache.vxquery.xmlquery.ast.BaseUriDeclNode;
import org.apache.vxquery.xmlquery.ast.BoundarySpaceDeclNode;
import org.apache.vxquery.xmlquery.ast.CDataSectionNode;
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
import org.apache.vxquery.xmlquery.ast.InfixExprNode.InfixOperator;
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
import org.apache.vxquery.xmlquery.ast.QueryBodyNode;
import org.apache.vxquery.xmlquery.ast.RelativePathExprNode;
import org.apache.vxquery.xmlquery.ast.SchemaImportNode;
import org.apache.vxquery.xmlquery.ast.SequenceTypeNode;
import org.apache.vxquery.xmlquery.ast.SingleTypeNode;
import org.apache.vxquery.xmlquery.ast.TypeDeclNode;
import org.apache.vxquery.xmlquery.ast.TypeExprNode;
import org.apache.vxquery.xmlquery.ast.UnaryExprNode;
import org.apache.vxquery.xmlquery.ast.UnorderedExprNode;
import org.apache.vxquery.xmlquery.ast.ValidateExprNode;
import org.apache.vxquery.xmlquery.ast.VarDeclNode;
import org.apache.vxquery.xmlquery.ast.VarRefNode;
import org.apache.vxquery.xmlquery.ast.VersionDeclNode;
import org.apache.vxquery.xmlquery.ast.WhereClauseNode;
import org.apache.vxquery.xmlquery.query.Module;
import org.apache.vxquery.xmlquery.query.ModuleType;
import org.apache.vxquery.xmlquery.query.XMLQueryCompilerConstants;
import org.apache.vxquery.xmlquery.query.XQueryConstants;
import org.apache.vxquery.xmlquery.query.XQueryConstants.PathType;
import org.apache.vxquery.xmlquery.query.XQueryConstants.TypeQuantifier;

import edu.uci.ics.hyracks.algebricks.core.algebra.base.ILogicalExpression;
import edu.uci.ics.hyracks.algebricks.core.algebra.base.ILogicalOperator;
import edu.uci.ics.hyracks.algebricks.core.algebra.base.ILogicalPlan;
import edu.uci.ics.hyracks.algebricks.core.algebra.base.LogicalVariable;
import edu.uci.ics.hyracks.algebricks.core.algebra.expressions.AggregateFunctionCallExpression;
import edu.uci.ics.hyracks.algebricks.core.algebra.expressions.ConstantExpression;
import edu.uci.ics.hyracks.algebricks.core.algebra.expressions.ScalarFunctionCallExpression;
import edu.uci.ics.hyracks.algebricks.core.algebra.expressions.UnnestingFunctionCallExpression;
import edu.uci.ics.hyracks.algebricks.core.algebra.expressions.VariableReferenceExpression;
import edu.uci.ics.hyracks.algebricks.core.algebra.operators.logical.AggregateOperator;
import edu.uci.ics.hyracks.algebricks.core.algebra.operators.logical.AssignOperator;
import edu.uci.ics.hyracks.algebricks.core.algebra.operators.logical.EmptyTupleSourceOperator;
import edu.uci.ics.hyracks.algebricks.core.algebra.operators.logical.NestedTupleSourceOperator;
import edu.uci.ics.hyracks.algebricks.core.algebra.operators.logical.OrderOperator;
import edu.uci.ics.hyracks.algebricks.core.algebra.operators.logical.SelectOperator;
import edu.uci.ics.hyracks.algebricks.core.algebra.operators.logical.SubplanOperator;
import edu.uci.ics.hyracks.algebricks.core.algebra.operators.logical.UnnestOperator;
import edu.uci.ics.hyracks.algebricks.core.algebra.plan.ALogicalPlanImpl;

public class XMLQueryTranslator {
    private static final Pattern UNQUOTER = Pattern
            .compile("(&lt;)|(&gt;)|(&apos;)|(&amp;)|(&quot;)|(\"\")|('')|(&#\\d+;)|(&#x(?:[A-Fa-f0-9])+;)");

    private final CompilerControlBlock ccb;

    private final StaticContext rootCtx;

    private StaticContext moduleCtx;

    private IVariableScope rootVarScope;

    private StaticContext currCtx;

    private int varCounter;

    public XMLQueryTranslator(CompilerControlBlock ccb) {
        this.ccb = ccb;
        varCounter = 0;
        rootCtx = ccb.getStaticContext();
    }

    private void pushContext() {
        currCtx = new ThinStaticContextImpl(currCtx);
    }

    private void popContext() {
        currCtx = currCtx.getParent();
    }

    public Module translateModule(ModuleNode moduleNode) throws SystemException {
        Module module = new Module();

        moduleCtx = new StaticContextImpl(rootCtx);
        moduleCtx.registerVariable(new XQueryVariable(XMLQueryCompilerConstants.DOT_VAR_NAME, SequenceType.create(
                AnyItemType.INSTANCE, Quantifier.QUANT_ONE), newLogicalVariable()));
        rootVarScope = new RootVariableScope();
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

        Pair<ILogicalPlan, LogicalVariable> planAndVar = null;
        switch (moduleNode.getTag()) {
            case LIBRARY_MODULE:
                throw new SystemException(ErrorCode.TODO);

            case MAIN_MODULE:
                planAndVar = translateMainModule((MainModuleNode) moduleNode);
        }

        module.setBody(planAndVar.getLeft());
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
                        SequenceType type = SequenceType.create(AnyItemType.INSTANCE, Quantifier.QUANT_STAR);
                        if (node.getType() != null) {
                            type = createSequenceType(node.getType());
                        }
                        LogicalVariable lVar = newLogicalVariable();
                        XQueryVariable var = new XQueryVariable(name, type, lVar);
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
                            paramTypes[i] = Pair.<QName, SequenceType> of(pName, pType);
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
                        throw new SystemException(ErrorCode.TODO);
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
                            TranslationContext tCtx = new TranslationContext(null, new EmptyTupleSourceOperator());
                            XQueryVariable[] params = new XQueryVariable[arity];
                            for (int i = 0; i < arity; ++i) {
                                ParamNode pNode = node.getParameters().get(i);
                                QName pName = createQName(pNode.getName());
                                SequenceType pType = SequenceType.create(AnyItemType.INSTANCE, Quantifier.QUANT_STAR);
                                if (pNode.getType() != null) {
                                    pType = createSequenceType(pNode.getType());
                                }
                                XQueryVariable pVar = new XQueryVariable(pName, pType, newLogicalVariable());
                                params[i] = pVar;
                                tCtx.varScope.registerVariable(pVar);
                            }
                            f.setParameters(params);
                            LogicalVariable var = translateExpression(node.getBody(), tCtx);
                            ILogicalExpression expr = treat(vre(var), sign.getReturnType());
                            var = createAssignment(expr, tCtx);
                            f.setBody(new ALogicalPlanImpl(mutable(tCtx.op)));
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
                throw new UnsupportedOperationException("schema-attribute(...) is not supported");
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
                throw new UnsupportedOperationException("schema-element(...) is not supported");
            }

            default:
                throw new IllegalStateException("Unknown node: " + itemType.getTag());
        }
    }

    private Pair<ILogicalPlan, LogicalVariable> translateMainModule(MainModuleNode moduleNode) throws SystemException {
        QueryBodyNode qbn = moduleNode.getQueryBody();
        ASTNode queryBody = qbn.getExpression();
        TranslationContext tCtx = new TranslationContext(null, new EmptyTupleSourceOperator());
        LogicalVariable lVar = translateExpression(queryBody, tCtx);
        ALogicalPlanImpl lp = new ALogicalPlanImpl(mutable(tCtx.op));

        return Pair.<ILogicalPlan, LogicalVariable> of(lp, lVar);
    }

    private LogicalVariable translateExpression(ASTNode value, TranslationContext tCtx) throws SystemException {
        tCtx = tCtx.pushContext();
        LogicalVariable var = translateExpressionSimple(value, tCtx);
        tCtx.popContext();
        return var;
    }

    private LogicalVariable translateExpressionSimple(ASTNode value, TranslationContext tCtx) throws SystemException {
        switch (value.getTag()) {
            case EXPRESSION: {
                ExprNode node = (ExprNode) value;
                return createConcatenation(translateExpressionList(node.getExpressions(), tCtx), tCtx);
            }

            case UNARY_EXPRESSION: {
                UnaryExprNode ueNode = (UnaryExprNode) value;
                boolean neg = false;
                for (UnaryExprNode.Sign s : ueNode.getSigns()) {
                    if (UnaryExprNode.Sign.MINUS.equals(s)) {
                        neg = !neg;
                    }
                }
                LogicalVariable var = translateExpressionSimple(ueNode.getExpr(), tCtx);
                if (neg) {
                    ILogicalExpression nExpr = normalize(vre(var), BuiltinOperators.NUMERIC_UNARY_MINUS.getSignature()
                            .getParameterType(0));
                    ILogicalExpression negExpr = sfce(BuiltinOperators.NUMERIC_UNARY_MINUS, nExpr);
                    var = createAssignment(negExpr, tCtx);
                }
                return var;
            }

            case INFIX_EXPRESSION: {
                InfixExprNode ie = (InfixExprNode) value;
                Function operator = getOperator(ie.getOperator());
                Signature sign = operator.getSignature();
                LogicalVariable varLeft = translateExpression(ie.getLeftExpr(), tCtx);
                LogicalVariable varRight = translateExpression(ie.getRightExpr(), tCtx);
                ILogicalExpression arg1 = normalize(vre(varLeft), sign.getParameterType(0));
                ILogicalExpression arg2 = normalize(vre(varRight), sign.getParameterType(1));
                if (BuiltinOperators.EXCEPT.equals(operator) || BuiltinOperators.INTERSECT.equals(operator)) {
                    arg1 = sfce(BuiltinOperators.SORT_DISTINCT_NODES_ASC, arg1);
                    arg2 = sfce(BuiltinOperators.SORT_DISTINCT_NODES_ASC, arg2);
                }
                ILogicalExpression result = sfce(operator, arg1, arg2);
                if (BuiltinOperators.UNION.equals(operator)) {
                    result = sfce(BuiltinOperators.SORT_DISTINCT_NODES_ASC, result);
                }
                return createAssignment(result, tCtx);
            }

            case ENCLOSED_EXPRESSION: {
                EnclosedExprNode ee = (EnclosedExprNode) value;
                return translateExpressionSimple(ee.getExpression(), tCtx);
            }

            case PATH_EXPRESSION:
                return translatePathExpr((PathExprNode) value, tCtx);

            case FUNCTION_EXPRESSION: {
                FunctionExprNode fnNode = (FunctionExprNode) value;
                List<LogicalVariable> args = new ArrayList<LogicalVariable>();
                for (ASTNode an : fnNode.getArguments()) {
                    args.add(translateExpression(an, tCtx));
                }
                QName name = createQName(fnNode.getName());
                SchemaType type = moduleCtx.lookupSchemaType(name);
                if (type != null && args.size() < 2) {
                    if (!type.isAtomicType()) {
                        throw new SystemException(ErrorCode.XPST0051, fnNode.getName().getSourceLocation());
                    }
                    LogicalVariable var = args.isEmpty() ? tCtx.varScope.lookupVariable(
                            XMLQueryCompilerConstants.DOT_VAR_NAME).getLogicalVariable() : args.get(0);
                    ILogicalExpression expr = cast(vre(var),
                            SequenceType.create((ItemType) type, Quantifier.QUANT_QUESTION));
                    return createAssignment(expr, tCtx);
                }
                QName fName = createQName(fnNode.getName(), moduleCtx.getDefaultFunctionNamespaceUri());
                if (BuiltinFunctions.FN_POSITION_QNAME.equals(fName)) {
                    XQueryVariable var = tCtx.varScope.lookupVariable(XMLQueryCompilerConstants.POS_VAR_NAME);
                    return var.getLogicalVariable();
                }
                if (BuiltinFunctions.FN_LAST_QNAME.equals(fName)) {
                    XQueryVariable var = tCtx.varScope.lookupVariable(XMLQueryCompilerConstants.LAST_VAR_NAME);
                    return var.getLogicalVariable();
                }
                int nArgs = fnNode.getArguments().size();
                Function fn = moduleCtx.lookupFunction(fName, nArgs);
                if (fn != null && fn.useContextImplicitly()) {
                    args.add(tCtx.varScope.lookupVariable(XMLQueryCompilerConstants.DOT_VAR_NAME).getLogicalVariable());
                    fn = moduleCtx.lookupFunction(fName, nArgs + 1);
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
                List<Mutable<ILogicalExpression>> argExprs = new ArrayList<Mutable<ILogicalExpression>>();
                for (int i = 0; i < args.size(); ++i) {
                    SequenceType argType = sign.getParameterType(i);
                    argExprs.add(mutable(normalize(vre(args.get(i)), argType)));
                }
                return createAssignment(new ScalarFunctionCallExpression(fn, argExprs), tCtx);
            }

            case TYPE_EXPRESSION: {
                TypeExprNode teNode = (TypeExprNode) value;
                LogicalVariable var = translateExpressionSimple(teNode.getExpr(), tCtx);
                SequenceType type = createSequenceType(teNode.getType());
                ILogicalExpression expr = null;
                switch (teNode.getOperator()) {
                    case CAST:
                        expr = cast(vre(var), type);
                        break;

                    case CASTABLE:
                        expr = castable(vre(var), type);
                        break;

                    case INSTANCEOF:
                        expr = instanceOf(vre(var), type);
                        break;

                    case TREAT:
                        expr = treat(vre(var), type);
                        break;

                    default:
                        throw new IllegalStateException("Unknown type operator: " + teNode.getOperator());
                }
                return createAssignment(expr, tCtx);
            }

            case EXTENSION_EXPRESSION: {
                ExtensionExprNode eNode = (ExtensionExprNode) value;
                if (eNode.getExpr() == null) {
                    throw new SystemException(ErrorCode.XQST0079, eNode.getSourceLocation());
                }
                return translateExpressionSimple(eNode.getExpr(), tCtx);
            }

            case PARENTHESIZED_EXPRESSION: {
                ParenthesizedExprNode peNode = (ParenthesizedExprNode) value;
                ASTNode eNode = peNode.getExpr();
                if (eNode == null) {
                    return createConcatenation(Collections.<LogicalVariable> emptyList(), tCtx);
                }
                return translateExpressionSimple(((ParenthesizedExprNode) value).getExpr(), tCtx);
            }

            case LITERAL: {
                LiteralNode lNode = (LiteralNode) value;
                String image = lNode.getImage();
                LiteralNode.LiteralType lType = lNode.getType();
                SequenceType t = null;
                switch (lType) {
                    case DECIMAL:
                        t = SequenceType.create(BuiltinTypeRegistry.XS_DECIMAL, Quantifier.QUANT_ONE);
                        break;
                    case DOUBLE:
                        t = SequenceType.create(BuiltinTypeRegistry.XS_DOUBLE, Quantifier.QUANT_ONE);
                        break;
                    case INTEGER:
                        t = SequenceType.create(BuiltinTypeRegistry.XS_INTEGER, Quantifier.QUANT_ONE);
                        break;
                    case STRING:
                        t = SequenceType.create(BuiltinTypeRegistry.XS_STRING, Quantifier.QUANT_ONE);
                        image = unquote(image);
                        break;
                    default:
                        throw new IllegalStateException("Unknown type: " + lType);
                }
                return createAssignment(ce(t, image), tCtx);
            }

            case DIRECT_PI_CONSTRUCTOR: {
                DirectPIConstructorNode dpicNode = (DirectPIConstructorNode) value;
                String target = dpicNode.getTarget();
                String content = dpicNode.getContent();
                return createAssignment(
                        sfce(BuiltinOperators.PI_CONSTRUCTOR,
                                ce(SequenceType.create(BuiltinTypeRegistry.XS_STRING, Quantifier.QUANT_ONE), target),
                                ce(SequenceType.create(BuiltinTypeRegistry.XS_STRING, Quantifier.QUANT_ONE), content)),
                        tCtx);
            }

            case DIRECT_COMMENT_CONSTRUCTOR: {
                DirectCommentConstructorNode dccNode = (DirectCommentConstructorNode) value;
                String content = dccNode.getContent();
                return createAssignment(
                        sfce(BuiltinOperators.COMMENT_CONSTRUCTOR,
                                ce(SequenceType.create(BuiltinTypeRegistry.XS_STRING, Quantifier.QUANT_ONE), content)),
                        tCtx);
            }

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

                        currCtx.registerNamespaceUri(aName.getLocalName(),
                                unquote(((ContentCharsNode) values.get(0)).getContent()));
                    }
                }
                List<ILogicalExpression> content = new ArrayList<ILogicalExpression>();
                for (DirectAttributeConstructorNode acNode : decNode.getAttributes()) {
                    QNameNode aName = acNode.getName();
                    if (!"xmlns".equals(aName.getPrefix())) {
                        content.add(vre(translateExpression(acNode, tCtx)));
                    }
                }
                ILogicalExpression name = ce(SequenceType.create(BuiltinTypeRegistry.XS_QNAME, Quantifier.QUANT_ONE),
                        createQName(startName, moduleCtx.getDefaultElementNamespaceUri()).toString());
                for (ASTNode cVal : decNode.getContent()) {
                    switch (cVal.getTag()) {
                        case CONTENT_CHARS: {
                            String contentChars = ((ContentCharsNode) cVal).getContent();
                            ILogicalExpression cce = ce(
                                    SequenceType.create(BuiltinTypeRegistry.XS_UNTYPED_ATOMIC, Quantifier.QUANT_ONE),
                                    contentChars);
                            content.add(sfce(BuiltinOperators.TEXT_CONSTRUCTOR, cce));
                            break;
                        }

                        default:
                            content.add(vre(translateExpression(cVal, tCtx)));
                    }
                }
                popContext();
                ILogicalExpression contentExpr = content.size() == 1 ? content.get(0) : sfce(
                        BuiltinOperators.CONCATENATE, content.toArray(new ILogicalExpression[content.size()]));
                return createAssignment(sfce(BuiltinOperators.ELEMENT_CONSTRUCTOR, name, contentExpr), tCtx);
            }

            case DIRECT_ATTRIBUTE_CONSTRUCTOR: {
                DirectAttributeConstructorNode dacNode = (DirectAttributeConstructorNode) value;
                QName aQName = createQName(dacNode.getName());
                ILogicalExpression name = ce(SequenceType.create(BuiltinTypeRegistry.XS_QNAME, Quantifier.QUANT_ONE),
                        aQName.toString());
                List<ILogicalExpression> content = new ArrayList<ILogicalExpression>();
                for (ASTNode aVal : dacNode.getValue()) {
                    switch (aVal.getTag()) {
                        case CONTENT_CHARS: {
                            String contentChars = ((ContentCharsNode) aVal).getContent();
                            ILogicalExpression cce = ce(
                                    SequenceType.create(BuiltinTypeRegistry.XS_UNTYPED_ATOMIC, Quantifier.QUANT_ONE),
                                    contentChars);
                            content.add(cce);
                            break;
                        }

                        default:
                            content.add(vre(translateExpression(aVal, tCtx)));
                    }
                }
                ILogicalExpression contentExpr = content.size() == 1 ? content.get(0) : sfce(
                        BuiltinOperators.CONCATENATE, content.toArray(new ILogicalExpression[content.size()]));
                return createAssignment(sfce(BuiltinOperators.ATTRIBUTE_CONSTRUCTOR, name, contentExpr), tCtx);
            }

            case CONTEXT_ITEM:
                return tCtx.varScope.lookupVariable(XMLQueryCompilerConstants.DOT_VAR_NAME).getLogicalVariable();

            case IF_EXPRESSION: {
                IfExprNode ieNode = (IfExprNode) value;
                ILogicalExpression cond = sfce(BuiltinFunctions.FN_BOOLEAN_1,
                        vre(translateExpression(ieNode.getIfExpr(), tCtx)));
                ILogicalExpression tExpr = vre(translateExpression(ieNode.getThenExpr(), tCtx));
                ILogicalExpression eExpr = vre(translateExpression(ieNode.getElseExpr(), tCtx));
                return createAssignment(sfce(BuiltinOperators.IF_THEN_ELSE, cond, tExpr, eExpr), tCtx);
            }

            case VARIABLE_REFERENCE: {
                VarRefNode vrNode = (VarRefNode) value;
                QName vName = createQName(vrNode.getVariable());
                XQueryVariable var = tCtx.varScope.lookupVariable(vName);
                if (var == null) {
                    throw new SystemException(ErrorCode.XPST0008, vrNode.getSourceLocation());
                }
                return createAssignment(treat(vre(var.getLogicalVariable()), var.getType()), tCtx);
            }

            case FLWOR_EXPRESSION: {
                FLWORExprNode fNode = (FLWORExprNode) value;
                List<FLWORClauseNode> cNodes = fNode.getClauses();
                int pushCount = 0;
                for (FLWORClauseNode cNode : cNodes) {
                    switch (cNode.getTag()) {
                        case FOR_CLAUSE: {
                            ForClauseNode fcNode = (ForClauseNode) cNode;
                            for (ForVarDeclNode fvdNode : fcNode.getVariables()) {
                                ILogicalExpression seq = vre(translateExpression(fvdNode.getSequence(), tCtx));
                                tCtx.pushVariableScope();
                                LogicalVariable forLVar = newLogicalVariable();
                                LogicalVariable posLVar = newLogicalVariable();
                                UnnestOperator unnest = new UnnestOperator(forLVar, mutable(ufce(
                                        BuiltinOperators.ITERATE, seq)), posLVar, null);
                                SequenceType forVarType = SequenceType.create(AnyItemType.INSTANCE,
                                        Quantifier.QUANT_ONE);
                                if (fvdNode.getType() != null) {
                                    forVarType = createSequenceType(fvdNode.getType());
                                }
                                XQueryVariable forVar = new XQueryVariable(createQName(fvdNode.getForVar()),
                                        forVarType, forLVar);
                                tCtx.varScope.registerVariable(forVar);
                                XQueryVariable posVar = null;
                                if (fvdNode.getPosVar() != null) {
                                    posVar = new XQueryVariable(createQName(fvdNode.getPosVar()), SequenceType.create(
                                            BuiltinTypeRegistry.XS_INTEGER, Quantifier.QUANT_ONE), posLVar);
                                    tCtx.varScope.registerVariable(posVar);
                                }
                                assert fvdNode.getScoreVar() == null;
                                unnest.getInputs().add(mutable(tCtx.op));
                                tCtx.op = unnest;
                                ++pushCount;
                            }
                            break;
                        }
                        case LET_CLAUSE: {
                            LetClauseNode lcNode = (LetClauseNode) cNode;
                            for (LetVarDeclNode lvdNode : lcNode.getVariables()) {
                                LogicalVariable seqVar = translateExpression(lvdNode.getSequence(), tCtx);
                                tCtx.pushVariableScope();
                                SequenceType letVarType = SequenceType.create(AnyItemType.INSTANCE,
                                        Quantifier.QUANT_ONE);
                                if (lvdNode.getType() != null) {
                                    letVarType = createSequenceType(lvdNode.getType());
                                }
                                XQueryVariable letVar = new XQueryVariable(createQName(lvdNode.getLetVar()),
                                        letVarType, seqVar);
                                tCtx.varScope.registerVariable(letVar);
                                ++pushCount;
                            }
                            break;
                        }
                        case WHERE_CLAUSE: {
                            WhereClauseNode wcNode = (WhereClauseNode) cNode;
                            ILogicalExpression condExpr = sfce(BuiltinFunctions.FN_BOOLEAN_1,
                                    vre(translateExpression(wcNode.getCondition(), tCtx)));
                            SelectOperator select = new SelectOperator(mutable(condExpr));
                            select.getInputs().add(mutable(tCtx.op));
                            tCtx.op = select;
                            break;
                        }
                        case ORDERBY_CLAUSE: {
                            OrderbyClauseNode ocNode = (OrderbyClauseNode) cNode;
                            List<edu.uci.ics.hyracks.algebricks.core.utils.Pair<OrderOperator.IOrder, Mutable<ILogicalExpression>>> oExprs = new ArrayList<edu.uci.ics.hyracks.algebricks.core.utils.Pair<OrderOperator.IOrder, Mutable<ILogicalExpression>>>();
                            List<String> collations = new ArrayList<String>();
                            for (OrderSpecNode osNode : ocNode.getOrderSpec()) {
                                ILogicalExpression oExpr = vre(translateExpression(osNode.getExpression(), tCtx));
                                OrderOperator.IOrder o = OrderOperator.ASC_ORDER;
                                XQueryConstants.OrderDirection oDir = osNode.getDirection();
                                if (oDir != null) {
                                    switch (oDir) {
                                        case ASCENDING:
                                            o = OrderOperator.ASC_ORDER;
                                            break;
                                        case DESCENDING:
                                            o = OrderOperator.DESC_ORDER;
                                            break;
                                    }
                                }
                                /*
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
                                */
                                oExprs.add(new edu.uci.ics.hyracks.algebricks.core.utils.Pair<OrderOperator.IOrder, Mutable<ILogicalExpression>>(
                                        o, mutable(oExpr)));
                            }
                            OrderOperator order = new OrderOperator(oExprs);
                            order.getInputs().add(mutable(tCtx.op));
                            tCtx.op = order;
                            break;
                        }
                        default:
                            throw new IllegalStateException("Unknown clause: " + cNode.getTag());
                    }
                }
                ILogicalExpression rExpr = vre(translateExpression(fNode.getReturnExpr(), tCtx));
                for (int i = 0; i < pushCount; ++i) {
                    tCtx.popVariableScope();
                }
                return createAssignment(rExpr, tCtx);
            }

            /*
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
                            Expression sExpr = ExpressionBuilder.functionCall(currCtx, BuiltinFunctions.FN_BOOLEAN_1,
                                    translateExpression(qeNode.getSatisfiesExpr()));
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
                        */

            case COMPUTED_TEXT_CONSTRUCTOR: {
                ComputedTextConstructorNode cNode = (ComputedTextConstructorNode) value;
                ASTNode content = cNode.getContent();
                return createAssignment(
                        sfce(BuiltinOperators.TEXT_CONSTRUCTOR, content == null ? sfce(BuiltinOperators.CONCATENATE)
                                : vre(translateExpression(content, tCtx))), tCtx);
            }

            case COMPUTED_PI_CONSTRUCTOR: {
                ComputedPIConstructorNode cNode = (ComputedPIConstructorNode) value;
                ASTNode content = cNode.getContent();
                return createAssignment(
                        sfce(BuiltinOperators.TEXT_CONSTRUCTOR,
                                vre(translateExpression(cNode.getTarget(), tCtx)),
                                content == null ? sfce(BuiltinOperators.CONCATENATE) : vre(translateExpression(content,
                                        tCtx))), tCtx);
            }

            case COMPUTED_COMMENT_CONSTRUCTOR: {
                ComputedCommentConstructorNode cNode = (ComputedCommentConstructorNode) value;
                ASTNode content = cNode.getContent();
                return createAssignment(
                        sfce(BuiltinOperators.COMMENT_CONSTRUCTOR, content == null ? sfce(BuiltinOperators.CONCATENATE)
                                : vre(translateExpression(content, tCtx))), tCtx);
            }

            case COMPUTED_DOCUMENT_CONSTRUCTOR:
                return createAssignment(
                        sfce(BuiltinOperators.DOCUMENT_CONSTRUCTOR,
                                vre(translateExpression(((ComputedDocumentConstructorNode) value).getContent(), tCtx))),
                        tCtx);

            case COMPUTED_ELEMENT_CONSTRUCTOR: {
                ComputedElementConstructorNode cNode = (ComputedElementConstructorNode) value;
                ILogicalExpression name = cast(vre(translateExpression(cNode.getName(), tCtx)),
                        SequenceType.create(BuiltinTypeRegistry.XS_QNAME, Quantifier.QUANT_ONE));
                ASTNode content = cNode.getContent();
                ILogicalExpression cExpr = content == null ? sfce(BuiltinOperators.CONCATENATE)
                        : vre(translateExpression(content, tCtx));
                return createAssignment(sfce(BuiltinOperators.ELEMENT_CONSTRUCTOR, name, cExpr), tCtx);
            }

            case COMPUTED_ATTRIBUTE_CONSTRUCTOR: {
                ComputedAttributeConstructorNode cNode = (ComputedAttributeConstructorNode) value;
                ILogicalExpression name = cast(vre(translateExpression(cNode.getName(), tCtx)),
                        SequenceType.create(BuiltinTypeRegistry.XS_QNAME, Quantifier.QUANT_ONE));
                ASTNode content = cNode.getContent();
                ILogicalExpression cExpr = content == null ? sfce(BuiltinOperators.CONCATENATE)
                        : vre(translateExpression(content, tCtx));
                return createAssignment(sfce(BuiltinOperators.ATTRIBUTE_CONSTRUCTOR, name, cExpr), tCtx);
            }

            case QNAME:
                return createAssignment(
                        ce(SequenceType.create(BuiltinTypeRegistry.XS_QNAME, Quantifier.QUANT_ONE),
                                createQName((QNameNode) value)), tCtx);

            case NCNAME:
                return createAssignment(
                        ce(SequenceType.create(BuiltinTypeRegistry.XS_STRING, Quantifier.QUANT_ONE),
                                ((NCNameNode) value).getName()), tCtx);

            case CDATA_SECTION:
                return createAssignment(
                        sfce(BuiltinOperators.TEXT_CONSTRUCTOR,
                                ce(SequenceType.create(BuiltinTypeRegistry.XS_STRING, Quantifier.QUANT_ONE),
                                        ((CDataSectionNode) value).getContent())), tCtx);

            case ORDERED_EXPRESSION:
                return createAssignment(
                        sfce(BuiltinOperators.ORDERED,
                                vre(translateExpression(((OrderedExprNode) value).getExpr(), tCtx))), tCtx);

            case UNORDERED_EXPRESSION:
                return createAssignment(
                        sfce(BuiltinOperators.UNORDERED,
                                vre(translateExpression(((UnorderedExprNode) value).getExpr(), tCtx))), tCtx);

            case VALIDATE_EXPRESSION: {
                ValidateExprNode vNode = (ValidateExprNode) value;
                XQueryConstants.ValidationMode mode = vNode.getMode();
                Function fn = mode == null || XQueryConstants.ValidationMode.STRICT.equals(mode) ? BuiltinOperators.VALIDATE_STRICT
                        : BuiltinOperators.VALIDATE_LAX;
                return createAssignment(sfce(fn, vre(translateExpression(vNode.getExpr(), tCtx))), tCtx);
            }
            default:
                throw new IllegalStateException("Unknown node: " + value.getTag());

        }

    }

    private LogicalVariable translatePathExpr(PathExprNode pe, TranslationContext tCtx) throws SystemException {
        ILogicalExpression ctxExpr = null;
        PathType type = pe.getPathType();
        if (type != null) {
            XQueryVariable dotVar = tCtx.varScope.lookupVariable(XMLQueryCompilerConstants.DOT_VAR_NAME);
            ILogicalExpression root = sfce(BuiltinFunctions.FN_ROOT_1, vre(dotVar.getLogicalVariable()));
            if (PathType.SLASH.equals(type)) {
                ctxExpr = root;
            } else {
                ctxExpr = sfce(BuiltinOperators.DESCENDANT_OR_SELF,
                        treat(root, SequenceType.create(AnyNodeType.INSTANCE, Quantifier.QUANT_STAR)));
            }
        }

        if (pe.getPaths() != null) {
            for (RelativePathExprNode rpen : pe.getPaths()) {
                boolean asc = true;
                if (PathType.SLASH_SLASH.equals(rpen.getPathType())) {
                    ctxExpr = sfce(BuiltinOperators.DESCENDANT_OR_SELF,
                            treat(ctxExpr, SequenceType.create(AnyNodeType.INSTANCE, Quantifier.QUANT_STAR)));
                }
                boolean popScope = false;
                if (ctxExpr != null) {
                    popScope = true;
                    tCtx = tCtx.pushContext();
                    tCtx.pushVariableScope();
                    iterateOver(ctxExpr, tCtx);
                    ctxExpr = null;
                }

                List<ASTNode> predicates = null;

                ASTNode pathNode = rpen.getPath();
                if (ASTTag.AXIS_STEP.equals(pathNode.getTag())) {
                    AxisStepNode axisNode = (AxisStepNode) pathNode;
                    predicates = axisNode.getPredicates();
                    AxisStepNode.Axis axis = axisNode.getAxis();
                    if (ctxExpr == null) {
                        ctxExpr = vre(tCtx.varScope.lookupVariable(XMLQueryCompilerConstants.DOT_VAR_NAME)
                                .getLogicalVariable());
                    }
                    Function axisFn = translateAxis(axis);
                    NodeType nt = translateNodeTest(axis, axisNode.getNodeTest());
                    ctxExpr = sfce(axisFn,
                            treat(ctxExpr, SequenceType.create(AnyNodeType.INSTANCE, Quantifier.QUANT_STAR)),
                            ce(SequenceType.create(BuiltinTypeRegistry.XSEXT_TYPE, Quantifier.QUANT_ONE), nt));
                    asc = isForwardAxis(axis);
                } else if (ASTTag.FILTER_EXPRESSION.equals(pathNode.getTag())) {
                    FilterExprNode filterNode = (FilterExprNode) pathNode;
                    predicates = filterNode.getPredicates();
                    ctxExpr = vre(translateExpression(filterNode.getExpr(), tCtx));
                } else {
                    throw new IllegalStateException("Unknown path node: " + pathNode.getTag());
                }
                if (predicates != null && !predicates.isEmpty()) {
                    ctxExpr = sfce(asc ? BuiltinOperators.SORT_DISTINCT_NODES_ASC_OR_ATOMICS
                            : BuiltinOperators.SORT_DISTINCT_NODES_DESC_OR_ATOMICS, ctxExpr);
                    for (ASTNode pn : predicates) {
                        tCtx = tCtx.pushContext();
                        tCtx.pushVariableScope();
                        iterateOver(ctxExpr, tCtx);
                        LogicalVariable pLVar = translateExpression(pn, tCtx);
                        ILogicalExpression tTest = instanceOf(vre(pLVar),
                                SequenceType.create(BuiltinTypeRegistry.XSEXT_NUMERIC, Quantifier.QUANT_ONE));
                        ILogicalExpression posTest = sfce(BuiltinOperators.VALUE_EQ, vre(pLVar), vre(tCtx.varScope
                                .lookupVariable(XMLQueryCompilerConstants.POS_VAR_NAME).getLogicalVariable()));
                        ILogicalExpression boolTest = sfce(BuiltinFunctions.FN_BOOLEAN_1, vre(pLVar));

                        SelectOperator select = new SelectOperator(mutable(sfce(BuiltinOperators.IF_THEN_ELSE, tTest,
                                posTest, boolTest)));
                        select.getInputs().add(mutable(tCtx.op));
                        tCtx.op = select;
                        ctxExpr = vre(tCtx.varScope.lookupVariable(XMLQueryCompilerConstants.DOT_VAR_NAME)
                                .getLogicalVariable());
                        tCtx.popVariableScope();
                        tCtx = tCtx.popContext();
                    }
                }
                if (popScope) {
                    tCtx.popVariableScope();
                    List<LogicalVariable> vars = new ArrayList<LogicalVariable>();
                    List<Mutable<ILogicalExpression>> exprs = new ArrayList<Mutable<ILogicalExpression>>();
                    LogicalVariable var = newLogicalVariable();
                    vars.add(var);
                    exprs.add(mutable(afce(BuiltinOperators.SEQUENCE, false, ctxExpr)));
                    AggregateOperator aop = new AggregateOperator(vars, exprs);
                    aop.getInputs().add(mutable(tCtx.op));
                    tCtx.op = aop;
                    tCtx = tCtx.popContext();
                    ctxExpr = vre(var);
                    ctxExpr = sfce(asc ? BuiltinOperators.SORT_DISTINCT_NODES_ASC_OR_ATOMICS
                            : BuiltinOperators.SORT_DISTINCT_NODES_DESC_OR_ATOMICS, vre(var));
                }
            }
        }
        return createAssignment(ctxExpr, tCtx);
    }

    private void iterateOver(ILogicalExpression ctxExpr, TranslationContext tCtx) {
        LogicalVariable seqLVar = createAssignment(ctxExpr, tCtx);
        LogicalVariable lastLVar = createAssignment(sfce(BuiltinFunctions.FN_COUNT_1, vre(seqLVar)), tCtx);
        tCtx.varScope.registerVariable(new XQueryVariable(XMLQueryCompilerConstants.LAST_VAR_NAME, SequenceType.create(
                BuiltinTypeRegistry.XS_INTEGER, Quantifier.QUANT_ONE), lastLVar));
        LogicalVariable forLVar = newLogicalVariable();
        LogicalVariable posLVar = newLogicalVariable();
        UnnestOperator unnest = new UnnestOperator(forLVar, mutable(ufce(BuiltinOperators.ITERATE, vre(seqLVar))),
                posLVar, null);
        SequenceType forVarType = SequenceType.create(AnyItemType.INSTANCE, Quantifier.QUANT_ONE);
        XQueryVariable forVar = new XQueryVariable(XMLQueryCompilerConstants.DOT_VAR_NAME, forVarType, forLVar);
        tCtx.varScope.registerVariable(forVar);
        XQueryVariable posVar = new XQueryVariable(XMLQueryCompilerConstants.POS_VAR_NAME, SequenceType.create(
                BuiltinTypeRegistry.XS_INTEGER, Quantifier.QUANT_ONE), posLVar);
        tCtx.varScope.registerVariable(posVar);
        unnest.getInputs().add(mutable(tCtx.op));
        tCtx.op = unnest;
    }

    private boolean isForwardAxis(AxisStepNode.Axis axis) {
        switch (axis) {
            case ABBREV:
            case CHILD:
            case ABBREV_ATTRIBUTE:
            case ATTRIBUTE:
            case DESCENDANT:
            case DESCENDANT_OR_SELF:
            case FOLLOWING:
            case FOLLOWING_SIBLING:
            case SELF:
                return true;

            case ANCESTOR:
            case ANCESTOR_OR_SELF:
            case DOT_DOT:
            case PARENT:
            case PRECEDING:
            case PRECEDING_SIBLING:
                return false;

            default:
                throw new IllegalStateException("Unknown axis: " + axis);
        }
    }

    private NodeType translateNodeTest(AxisStepNode.Axis axis, ASTNode nodeTest) throws SystemException {
        NodeType nt = AnyNodeType.INSTANCE;
        if (nodeTest != null) {
            switch (nodeTest.getTag()) {
                case NAME_TEST: {
                    NameTestNode ntn = (NameTestNode) nodeTest;
                    String uri = null;
                    if (ntn.getPrefix() != null) {
                        if (!"".equals(ntn.getPrefix())) {
                            uri = currCtx.lookupNamespaceUri(ntn.getPrefix());
                            if (uri == null) {
                                throw new SystemException(ErrorCode.XPST0081, ntn.getSourceLocation());
                            }
                        } else {
                            uri = "";
                        }
                    }
                    NameTest nameTest = new NameTest(uri, ntn.getLocalName());
                    if (axis == AxisStepNode.Axis.ATTRIBUTE || axis == AxisStepNode.Axis.ABBREV_ATTRIBUTE) {
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
        return nt;
    }

    private Function translateAxis(AxisStepNode.Axis axis) {
        switch (axis) {
            case ABBREV:
            case CHILD:
                return BuiltinOperators.CHILD;

            case ABBREV_ATTRIBUTE:
            case ATTRIBUTE:
                return BuiltinOperators.ATTRIBUTE;

            case ANCESTOR:
                return BuiltinOperators.ANCESTOR;

            case ANCESTOR_OR_SELF:
                return BuiltinOperators.ANCESTOR_OR_SELF;

            case DESCENDANT:
                return BuiltinOperators.DESCENDANT;

            case DESCENDANT_OR_SELF:
                return BuiltinOperators.DESCENDANT_OR_SELF;

            case DOT_DOT:
            case PARENT:
                return BuiltinOperators.PARENT;

            case FOLLOWING:
                return BuiltinOperators.FOLLOWING;

            case FOLLOWING_SIBLING:
                return BuiltinOperators.FOLLOWING_SIBLING;

            case PRECEDING:
                return BuiltinOperators.PRECEDING;

            case PRECEDING_SIBLING:
                return BuiltinOperators.PRECEDING_SIBLING;

            case SELF:
                return BuiltinOperators.SELF;

            default:
                throw new IllegalStateException("Unknown axis: " + axis);
        }
    }

    private static String unquote(String image) throws SystemException {
        StringBuilder buffer = new StringBuilder();
        char quoteChar = image.charAt(0);
        image = image.substring(1, image.length() - 1);
        Matcher m = UNQUOTER.matcher(image);
        int i = 0;
        while (m.find()) {
            int start = m.start();
            int end = m.end();
            if (i < start) {
                buffer.append(image, i, start);
            }
            if (m.start(1) >= 0) {
                buffer.append('<');
            } else if (m.start(2) >= 0) {
                buffer.append('>');
            } else if (m.start(3) >= 0) {
                buffer.append('\'');
            } else if (m.start(4) >= 0) {
                buffer.append('&');
            } else if (m.start(5) >= 0) {
                buffer.append('"');
            } else if (m.start(6) >= 0) {
                buffer.append(quoteChar == '"' ? '"' : "\"\"");
            } else if (m.start(7) >= 0) {
                buffer.append(quoteChar == '\'' ? '\'' : "''");
            } else if (m.start(8) >= 0) {
                try {
                    buffer.appendCodePoint(Integer.parseInt(image.substring(start + 2, end - 1)));
                } catch (NumberFormatException e) {
                    throw new SystemException(ErrorCode.XQST0090);
                }
            } else if (m.start(9) >= 0) {
                try {
                    buffer.appendCodePoint(Integer.parseInt(image.substring(start + 3, end - 1), 16));
                } catch (NumberFormatException e) {
                    throw new SystemException(ErrorCode.XQST0090);
                }
            }
            i = m.end();
        }
        if (i < image.length()) {
            buffer.append(image, i, image.length());
        }
        return buffer.toString();
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

    private static ILogicalExpression afce(Function fn, boolean isTwoStep, ILogicalExpression... argExprs) {
        List<Mutable<ILogicalExpression>> args = new ArrayList<Mutable<ILogicalExpression>>();
        for (ILogicalExpression e : argExprs) {
            args.add(mutable(e));
        }
        return new AggregateFunctionCallExpression(fn, isTwoStep, args);
    }

    private static ILogicalExpression ufce(Function fn, ILogicalExpression... argExprs) {
        List<Mutable<ILogicalExpression>> args = new ArrayList<Mutable<ILogicalExpression>>();
        for (ILogicalExpression e : argExprs) {
            args.add(mutable(e));
        }
        return new UnnestingFunctionCallExpression(fn, args);
    }

    private static ILogicalExpression sfce(Function fn, ILogicalExpression... argExprs) {
        List<Mutable<ILogicalExpression>> args = new ArrayList<Mutable<ILogicalExpression>>();
        for (ILogicalExpression e : argExprs) {
            args.add(mutable(e));
        }
        return new ScalarFunctionCallExpression(fn, args);
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
                return BuiltinOperators.NODE_AFTER;

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
                return BuiltinOperators.NODE_BEFORE;

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

    private static ILogicalExpression ce(SequenceType type, Object value) {
        return new ConstantExpression(new ConstantValue(type, value));
    }

    private static ILogicalExpression vre(LogicalVariable var) {
        if (var == null) {
            throw new NullPointerException();
        }
        return new VariableReferenceExpression(var);
    }

    private LogicalVariable createConcatenation(List<LogicalVariable> vars, TranslationContext tCtx) {
        if (vars.size() == 1) {
            return vars.get(0);
        }
        return createFunctionCall(BuiltinOperators.CONCATENATE, vars, tCtx);
    }

    private LogicalVariable createFunctionCall(Function fn, List<LogicalVariable> vars, TranslationContext tCtx) {
        return createAssignment(createFunctionCall(fn, vars), tCtx);
    }

    private LogicalVariable createAssignment(ILogicalExpression expr, TranslationContext tCtx) {
        LogicalVariable result = newLogicalVariable();
        AssignOperator aOp = new AssignOperator(result, mutable(expr));
        aOp.getInputs().add(mutable(tCtx.op));
        tCtx.op = aOp;
        return result;
    }

    private static ILogicalExpression createFunctionCall(Function fn, List<LogicalVariable> vars) {
        List<Mutable<ILogicalExpression>> args = new ArrayList<Mutable<ILogicalExpression>>();
        for (LogicalVariable var : vars) {
            args.add(mutable(new VariableReferenceExpression(var)));
        }
        return new ScalarFunctionCallExpression(fn, args);
    }

    private static ILogicalExpression normalize(ILogicalExpression expr, SequenceType type) {
        if (type.getItemType().isAtomicType()) {
            ILogicalExpression atomizedExpr = new ScalarFunctionCallExpression(BuiltinFunctions.FN_DATA_1,
                    Collections.singletonList(mutable(expr)));
            AtomicType aType = (AtomicType) type.getItemType();
            if (TypeUtils.isSubtypeTypeOf(aType, BuiltinTypeRegistry.XS_BOOLEAN)) {
                return new ScalarFunctionCallExpression(BuiltinFunctions.FN_BOOLEAN_1,
                        Collections.singletonList(mutable(atomizedExpr)));
            }
            return promote(atomizedExpr, type);
        } else {
            return treat(expr, type);
        }
    }

    private static ILogicalExpression promote(ILogicalExpression expr, SequenceType type) {
        // TODO Fix.
        return expr;
    }

    private static ILogicalExpression treat(ILogicalExpression expr, SequenceType type) {
        // TODO Fix.
        return expr;
    }

    private static ILogicalExpression cast(ILogicalExpression expr, SequenceType type) {
        // TODO Fix.
        return expr;
    }

    private static ILogicalExpression castable(ILogicalExpression expr, SequenceType type) {
        // TODO Fix.
        return expr;
    }

    private static ILogicalExpression instanceOf(ILogicalExpression expr, SequenceType type) {
        // TODO Fix.
        return expr;
    }

    private List<LogicalVariable> translateExpressionList(List<ASTNode> expressions, TranslationContext tCtx)
            throws SystemException {
        List<LogicalVariable> result = new ArrayList<LogicalVariable>();
        for (ASTNode e : expressions) {
            result.add(translateExpression(e, tCtx));
        }
        return result;
    }

    private static Mutable<ILogicalExpression> mutable(ILogicalExpression expr) {
        return new MutableObject<ILogicalExpression>(expr);
    }

    private static Mutable<ILogicalOperator> mutable(ILogicalOperator op) {
        return new MutableObject<ILogicalOperator>(op);
    }

    private LogicalVariable newLogicalVariable() {
        return new LogicalVariable(varCounter++);
    }

    private class RootVariableScope implements IVariableScope {
        @Override
        public IVariableScope getParentScope() {
            return null;
        }

        @Override
        public XQueryVariable lookupVariable(QName name) {
            return moduleCtx.lookupVariable(name);
        }

        @Override
        public void registerVariable(XQueryVariable var) {
            moduleCtx.registerVariable(var);
        }
    }

    private class TranslationContext {
        private final TranslationContext parent;

        private ILogicalOperator op;

        private IVariableScope varScope;

        public TranslationContext(TranslationContext parent, ILogicalOperator op) {
            this.parent = parent;
            this.op = op;
            varScope = parent == null ? rootVarScope : parent.varScope;
        }

        TranslationContext pushContext() {
            SubplanOperator sOp = new SubplanOperator();
            sOp.getInputs().add(mutable(op));
            op = sOp;
            NestedTupleSourceOperator ntsOp = new NestedTupleSourceOperator(mutable(sOp));
            TranslationContext childCtx = new TranslationContext(this, ntsOp);
            return childCtx;
        }

        TranslationContext popContext() {
            SubplanOperator sOp = (SubplanOperator) parent.op;
            sOp.setRootOp(mutable(op));
            return parent;
        }

        void pushVariableScope() {
            varScope = new ExpressionVariableScope(varScope);
        }

        void popVariableScope() {
            varScope = varScope.getParentScope();
        }
    }

    private interface IVariableScope {
        public IVariableScope getParentScope();

        public XQueryVariable lookupVariable(QName name);

        public void registerVariable(XQueryVariable var);
    }

    private static class ExpressionVariableScope implements IVariableScope {
        private final IVariableScope parent;

        private final Map<QName, XQueryVariable> varMap;

        public ExpressionVariableScope(IVariableScope parent) {
            this.parent = parent;
            varMap = new HashMap<QName, XQueryVariable>();
        }

        @Override
        public IVariableScope getParentScope() {
            return parent;
        }

        @Override
        public XQueryVariable lookupVariable(QName name) {
            if (varMap.containsKey(name)) {
                return varMap.get(name);
            }
            return parent.lookupVariable(name);
        }

        @Override
        public void registerVariable(XQueryVariable var) {
            varMap.put(var.getName(), var);
        }
    }
}