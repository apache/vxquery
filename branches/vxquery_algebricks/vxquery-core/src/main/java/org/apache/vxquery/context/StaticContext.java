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

public interface StaticContext {
    public DataspaceContext getDataspaceContext();

    public StaticContext getParent();

    public String lookupNamespaceUri(String prefix);

    public void registerNamespaceUri(String prefix, String uri);

    public Collation lookupCollation(String collationName);

    public void registerCollation(String collationName, Collation collation);

    public Function lookupFunction(QName functionName, int arity);

    public Function[] lookupFunctions(QName functionName);

    public void registerFunction(Function function);

    public Iterator<Function> listFunctions();

    public SequenceType lookupDocumentType(String docUri);

    public void registerDocumentType(String docUri, SequenceType type);

    public XQueryVariable lookupVariable(QName name);

    public void registerVariable(XQueryVariable var);

    public Iterator<XQueryVariable> listVariables();

    public SequenceType lookupCollectionType(String collectionUri);

    public void registerCollectionType(String collectionUri, SequenceType type);

    public Iterator<Pair<String, List<String>>> listModules();

    public void registerModuleImport(String uri, List<String> locations);

    public Iterator<Pair<String, List<String>>> listSchemas();

    public void registerSchemaImport(String uri, List<String> locations);

    public SchemaType lookupSchemaType(QName name);

    public void registerSchemaType(QName name, SchemaType type);

    public int lookupSequenceType(SequenceType type);

    public int encodeSequenceType(SequenceType type);

    public int getMaxSequenceTypeCode();

    public AttributeType lookupAttributeDeclaration(QName name);

    public void registerAttributeDeclaration(QName name, AttributeType attrDecl);

    public ElementType lookupElementDeclaration(QName name);

    public void registerElementDeclaration(QName name, ElementType elemDecl);

    public BoundarySpaceProperty getBoundarySpaceProperty();

    public void setBoundarySpaceProperty(BoundarySpaceProperty boundarySpaceProperty);

    public String getDefaultFunctionNamespaceUri();

    public void setDefaultFunctionNamespaceUri(String uri);

    public String getDefaultElementNamespaceUri();

    public void setDefaultElementNamespaceUri(String uri);

    public OrderingModeProperty getOrderingModeProperty();

    public void setOrderingModeProperty(OrderingModeProperty orderingMode);

    public EmptyOrderProperty getEmptyOrderProperty();

    public void setEmptyOrderProperty(EmptyOrderProperty emptyOrder);

    public String getDefaultCollation();

    public void setDefaultCollation(String defaultCollation);

    public String getBaseUri();

    public void setBaseUri(String baseUri);

    public ConstructionModeProperty getConstructionModeProperty();

    public void setConstructionModeProperty(ConstructionModeProperty constructionMode);

    public CopyNamespacesModeProperty getCopyNamespacesModeProperty();

    public void setCopyNamespacesModeProperty(CopyNamespacesModeProperty copyNamespacesMode);

    public SequenceType getDefaultCollectionType();

    public void setDefaultCollectionType(SequenceType type);

    public void setOption(QName name, String value);

    public String getOption(QName name);

    public enum BoundarySpaceProperty {
        PRESERVE,
        STRIP
    }

    public enum OrderingModeProperty {
        ORDERED,
        UNORDERED
    }

    public enum ConstructionModeProperty {
        PRESERVE,
        STRIP
    }

    public enum EmptyOrderProperty {
        GREATEST,
        LEAST
    }

    public enum CopyNamespacesModeProperty {
        PRESERVE_INHERIT,
        PRESERVE_NOINHERIT,
        NOPRESERVE_INHERIT,
        NOPRESERVE_NOINHERIT
    }
}