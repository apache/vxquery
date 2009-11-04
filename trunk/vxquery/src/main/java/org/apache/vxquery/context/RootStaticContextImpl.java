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

import java.util.List;
import java.util.Map;

import javax.xml.namespace.QName;

import org.apache.vxquery.collations.CodepointCollation;
import org.apache.vxquery.collations.Collation;
import org.apache.vxquery.functions.BuiltinFunctions;
import org.apache.vxquery.functions.Function;
import org.apache.vxquery.types.AttributeType;
import org.apache.vxquery.types.BuiltinTypeRegistry;
import org.apache.vxquery.types.ElementType;
import org.apache.vxquery.types.SchemaType;
import org.apache.vxquery.types.SequenceType;
import org.apache.vxquery.xmlquery.query.XQueryConstants;

public final class RootStaticContextImpl extends StaticContextImpl {
    public static final StaticContext INSTANCE;

    static {
        INSTANCE = new RootStaticContextImpl();

        // Namespaces
        INSTANCE.registerNamespaceUri(XQueryConstants.XML_PREFIX, XQueryConstants.XML_NSURI);
        INSTANCE.registerNamespaceUri(XQueryConstants.XS_PREFIX, XQueryConstants.XS_NSURI);
        INSTANCE.registerNamespaceUri(XQueryConstants.XSEXT_PREFIX, XQueryConstants.XSEXT_NSURI);
        INSTANCE.registerNamespaceUri(XQueryConstants.XSI_PREFIX, XQueryConstants.XSI_NSURI);
        INSTANCE.registerNamespaceUri(XQueryConstants.FN_PREFIX, XQueryConstants.FN_NSURI);
        INSTANCE.registerNamespaceUri(XQueryConstants.LOCAL_PREFIX, XQueryConstants.LOCAL_NSURI);

        INSTANCE.setBaseUri(".");

        INSTANCE.setDefaultFunctionNamespaceUri(XQueryConstants.FN_NSURI);

        // Types
        for (Map.Entry<QName, SchemaType> type : BuiltinTypeRegistry.TYPE_MAP.entrySet()) {
            INSTANCE.registerSchemaType(type.getKey(), type.getValue());
        }

        // Functions
        for (Function fn : BuiltinFunctions.FUNCTION_COLLECTION) {
            INSTANCE.registerFunction(fn);
        }
        
        INSTANCE.registerCollation(CodepointCollation.URI, CodepointCollation.INSTANCE);
        
        INSTANCE.setDefaultCollation(CodepointCollation.URI);

        ((RootStaticContextImpl) INSTANCE).sealed = true;
    }

    private boolean sealed;

    private RootStaticContextImpl() {
        super(null);
    }

    private void checkSealed() {
        if (sealed) {
            throw new IllegalStateException();
        }
    }

    @Override
    public void registerAttributeDeclaration(QName name, AttributeType attrDecl) {
        checkSealed();
        super.registerAttributeDeclaration(name, attrDecl);
    }

    @Override
    public void registerCollation(String collationName, Collation collation) {
        checkSealed();
        super.registerCollation(collationName, collation);
    }

    @Override
    public void registerCollectionType(String collectionUri, SequenceType type) {
        checkSealed();
        super.registerCollectionType(collectionUri, type);
    }

    @Override
    public void registerDocumentType(String docUri, SequenceType type) {
        checkSealed();
        super.registerDocumentType(docUri, type);
    }

    @Override
    public void registerElementDeclaration(QName name, ElementType elemDecl) {
        checkSealed();
        super.registerElementDeclaration(name, elemDecl);
    }

    @Override
    public void registerFunction(Function function) {
        checkSealed();
        super.registerFunction(function);
    }

    @Override
    public void registerNamespaceUri(String prefix, String uri) {
        checkSealed();
        super.registerNamespaceUri(prefix, uri);
    }

    @Override
    public void registerModuleImport(String uri, List<String> locations) {
        checkSealed();
        super.registerModuleImport(uri, locations);
    }

    @Override
    public void registerSchemaImport(String uri, List<String> locations) {
        checkSealed();
        super.registerSchemaImport(uri, locations);
    }

    @Override
    public void registerSchemaType(QName name, SchemaType type) {
        checkSealed();
        super.registerSchemaType(name, type);
    }

    @Override
    public void setBaseUri(String baseUri) {
        checkSealed();
        super.setBaseUri(baseUri);
    }

    @Override
    public void setBoundarySpaceProperty(BoundarySpaceProperty boundarySpaceProperty) {
        checkSealed();
        super.setBoundarySpaceProperty(boundarySpaceProperty);
    }

    @Override
    public void setConstructionModeProperty(ConstructionModeProperty constructionMode) {
        checkSealed();
        super.setConstructionModeProperty(constructionMode);
    }

    @Override
    public void setCopyNamespacesModeProperty(CopyNamespacesModeProperty copyNamespacesMode) {
        checkSealed();
        super.setCopyNamespacesModeProperty(copyNamespacesMode);
    }

    @Override
    public void setDefaultCollation(String defaultCollation) {
        checkSealed();
        super.setDefaultCollation(defaultCollation);
    }

    @Override
    public void setDefaultCollectionType(SequenceType type) {
        checkSealed();
        super.setDefaultCollectionType(type);
    }

    @Override
    public void setDefaultElementNamespaceUri(String uri) {
        checkSealed();
        super.setDefaultElementNamespaceUri(uri);
    }

    @Override
    public void setDefaultFunctionNamespaceUri(String uri) {
        checkSealed();
        super.setDefaultFunctionNamespaceUri(uri);
    }

    @Override
    public void setEmptyOrderProperty(EmptyOrderProperty emptyOrder) {
        checkSealed();
        super.setEmptyOrderProperty(emptyOrder);
    }

    @Override
    public void setOrderingModeProperty(OrderingModeProperty orderingMode) {
        checkSealed();
        super.setOrderingModeProperty(orderingMode);
    }
}