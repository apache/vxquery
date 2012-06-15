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

import java.util.Iterator;
import java.util.List;

import javax.xml.namespace.QName;

import org.apache.commons.lang3.tuple.Pair;
import org.apache.vxquery.collations.Collation;
import org.apache.vxquery.functions.Function;
import org.apache.vxquery.types.AttributeType;
import org.apache.vxquery.types.ElementType;
import org.apache.vxquery.types.SchemaType;
import org.apache.vxquery.types.SequenceType;

public abstract class DelegatingStaticContextImpl implements StaticContext {
    protected final StaticContext parent;

    public DelegatingStaticContextImpl(StaticContext parent) {
        this.parent = parent;
    }

    @Override
    public String getBaseUri() {
        return parent.getBaseUri();
    }

    @Override
    public BoundarySpaceProperty getBoundarySpaceProperty() {
        return parent.getBoundarySpaceProperty();
    }

    @Override
    public ConstructionModeProperty getConstructionModeProperty() {
        return parent.getConstructionModeProperty();
    }

    @Override
    public CopyNamespacesModeProperty getCopyNamespacesModeProperty() {
        return parent.getCopyNamespacesModeProperty();
    }

    @Override
    public DataspaceContext getDataspaceContext() {
        return parent.getDataspaceContext();
    }

    @Override
    public String getDefaultCollation() {
        return parent.getDefaultCollation();
    }

    @Override
    public SequenceType getDefaultCollectionType() {
        return parent.getDefaultCollectionType();
    }

    @Override
    public String getDefaultElementNamespaceUri() {
        return parent.getDefaultElementNamespaceUri();
    }

    @Override
    public String getDefaultFunctionNamespaceUri() {
        return parent.getDefaultFunctionNamespaceUri();
    }

    @Override
    public EmptyOrderProperty getEmptyOrderProperty() {
        return parent.getEmptyOrderProperty();
    }

    @Override
    public String getOption(QName name) {
        return parent.getOption(name);
    }

    @Override
    public OrderingModeProperty getOrderingModeProperty() {
        return parent.getOrderingModeProperty();
    }

    @Override
    public StaticContext getParent() {
        return parent;
    }

    @Override
    public Iterator<Function> listFunctions() {
        return parent.listFunctions();
    }

    @Override
    public Iterator<Pair<String, List<String>>> listModules() {
        return parent.listModules();
    }

    @Override
    public Iterator<Pair<String, List<String>>> listSchemas() {
        return parent.listSchemas();
    }

    @Override
    public AttributeType lookupAttributeDeclaration(QName name) {
        return parent.lookupAttributeDeclaration(name);
    }

    @Override
    public Collation lookupCollation(String collationName) {
        return parent.lookupCollation(collationName);
    }

    @Override
    public SequenceType lookupCollectionType(String collectionUri) {
        return parent.lookupCollectionType(collectionUri);
    }

    @Override
    public SequenceType lookupDocumentType(String docUri) {
        return parent.lookupDocumentType(docUri);
    }

    @Override
    public ElementType lookupElementDeclaration(QName name) {
        return parent.lookupElementDeclaration(name);
    }

    @Override
    public Function lookupFunction(QName functionName, int arity) {
        return parent.lookupFunction(functionName, arity);
    }

    @Override
    public Function[] lookupFunctions(QName functionName) {
        return parent.lookupFunctions(functionName);
    }

    @Override
    public String lookupNamespaceUri(String prefix) {
        return parent.lookupNamespaceUri(prefix);
    }

    @Override
    public SchemaType lookupSchemaType(QName name) {
        return parent.lookupSchemaType(name);
    }

    @Override
    public Iterator<XQueryVariable> listVariables() {
        return parent.listVariables();
    }

    @Override
    public XQueryVariable lookupVariable(QName name) {
        return parent.lookupVariable(name);
    }

    @Override
    public void registerAttributeDeclaration(QName name, AttributeType attrDecl) {
        parent.registerAttributeDeclaration(name, attrDecl);
    }

    @Override
    public void registerCollation(String collationName, Collation collation) {
        parent.registerCollation(collationName, collation);
    }

    @Override
    public void registerCollectionType(String collectionUri, SequenceType type) {
        parent.registerCollectionType(collectionUri, type);
    }

    @Override
    public void registerDocumentType(String docUri, SequenceType type) {
        parent.registerDocumentType(docUri, type);
    }

    @Override
    public void registerElementDeclaration(QName name, ElementType elemDecl) {
        parent.registerElementDeclaration(name, elemDecl);
    }

    @Override
    public void registerFunction(Function function) {
        parent.registerFunction(function);
    }

    @Override
    public void registerModuleImport(String uri, List<String> locations) {
        parent.registerModuleImport(uri, locations);
    }

    @Override
    public void registerNamespaceUri(String prefix, String uri) {
        parent.registerNamespaceUri(prefix, uri);
    }

    @Override
    public void registerSchemaImport(String uri, List<String> locations) {
        parent.registerSchemaImport(uri, locations);
    }

    @Override
    public void registerSchemaType(QName name, SchemaType type) {
        parent.registerSchemaType(name, type);
    }

    @Override
    public int lookupSequenceType(SequenceType type) {
        return parent.lookupSequenceType(type);
    }

    @Override
    public int encodeSequenceType(SequenceType type) {
        return parent.encodeSequenceType(type);
    }

    @Override
    public int getMaxSequenceTypeCode() {
        return parent.getMaxSequenceTypeCode();
    }

    @Override
    public void registerVariable(XQueryVariable var) {
        parent.registerVariable(var);
    }

    @Override
    public void setBaseUri(String baseUri) {
        parent.setBaseUri(baseUri);
    }

    @Override
    public void setBoundarySpaceProperty(BoundarySpaceProperty boundarySpaceProperty) {
        parent.setBoundarySpaceProperty(boundarySpaceProperty);
    }

    @Override
    public void setConstructionModeProperty(ConstructionModeProperty constructionMode) {
        parent.setConstructionModeProperty(constructionMode);
    }

    @Override
    public void setCopyNamespacesModeProperty(CopyNamespacesModeProperty copyNamespacesMode) {
        parent.setCopyNamespacesModeProperty(copyNamespacesMode);
    }

    @Override
    public void setDefaultCollation(String defaultCollation) {
        parent.setDefaultCollation(defaultCollation);
    }

    @Override
    public void setDefaultCollectionType(SequenceType type) {
        parent.setDefaultCollectionType(type);
    }

    @Override
    public void setDefaultElementNamespaceUri(String uri) {
        parent.setDefaultElementNamespaceUri(uri);
    }

    @Override
    public void setDefaultFunctionNamespaceUri(String uri) {
        parent.setDefaultFunctionNamespaceUri(uri);
    }

    @Override
    public void setEmptyOrderProperty(EmptyOrderProperty emptyOrder) {
        parent.setEmptyOrderProperty(emptyOrder);
    }

    @Override
    public void setOption(QName name, String value) {
        parent.setOption(name, value);
    }

    @Override
    public void setOrderingModeProperty(OrderingModeProperty orderingMode) {
        parent.setOrderingModeProperty(orderingMode);
    }
}