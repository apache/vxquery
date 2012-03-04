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
    public abstract DataspaceContext getDataspaceContext();

    public abstract StaticContext getParent();

    public String lookupNamespaceUri(String prefix);

    public void registerNamespaceUri(String prefix, String uri);

    public abstract Collation lookupCollation(String collationName);

    public abstract void registerCollation(String collationName, Collation collation);

    public abstract Function lookupFunction(QName functionName, int arity);

    public abstract Function[] lookupFunctions(QName functionName);

    public abstract void registerFunction(Function function);

    public abstract Iterator<Function> listFunctions();

    public abstract SequenceType lookupDocumentType(String docUri);

    public abstract void registerDocumentType(String docUri, SequenceType type);

    public abstract XQueryVariable lookupVariable(QName name);

    public abstract void registerVariable(XQueryVariable var);

    public abstract Iterator<XQueryVariable> listVariables();

    public abstract SequenceType lookupCollectionType(String collectionUri);

    public abstract void registerCollectionType(String collectionUri, SequenceType type);

    public abstract Iterator<Pair<String, List<String>>> listModules();

    public abstract void registerModuleImport(String uri, List<String> locations);

    public abstract Iterator<Pair<String, List<String>>> listSchemas();

    public abstract void registerSchemaImport(String uri, List<String> locations);

    public abstract SchemaType lookupSchemaType(QName name);

    public abstract void registerSchemaType(QName name, SchemaType type);

    public abstract AttributeType lookupAttributeDeclaration(QName name);

    public abstract void registerAttributeDeclaration(QName name, AttributeType attrDecl);

    public abstract ElementType lookupElementDeclaration(QName name);

    public abstract void registerElementDeclaration(QName name, ElementType elemDecl);

    public abstract BoundarySpaceProperty getBoundarySpaceProperty();

    public abstract void setBoundarySpaceProperty(BoundarySpaceProperty boundarySpaceProperty);

    public abstract String getDefaultFunctionNamespaceUri();

    public abstract void setDefaultFunctionNamespaceUri(String uri);

    public abstract String getDefaultElementNamespaceUri();

    public abstract void setDefaultElementNamespaceUri(String uri);

    public abstract OrderingModeProperty getOrderingModeProperty();

    public abstract void setOrderingModeProperty(OrderingModeProperty orderingMode);

    public abstract EmptyOrderProperty getEmptyOrderProperty();

    public abstract void setEmptyOrderProperty(EmptyOrderProperty emptyOrder);

    public abstract String getDefaultCollation();

    public abstract void setDefaultCollation(String defaultCollation);

    public abstract String getBaseUri();

    public abstract void setBaseUri(String baseUri);

    public abstract ConstructionModeProperty getConstructionModeProperty();

    public abstract void setConstructionModeProperty(ConstructionModeProperty constructionMode);

    public abstract CopyNamespacesModeProperty getCopyNamespacesModeProperty();

    public abstract void setCopyNamespacesModeProperty(CopyNamespacesModeProperty copyNamespacesMode);

    public abstract SequenceType getDefaultCollectionType();

    public abstract void setDefaultCollectionType(SequenceType type);

    public abstract void setOption(QName name, String value);

    public abstract String getOption(QName name);

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