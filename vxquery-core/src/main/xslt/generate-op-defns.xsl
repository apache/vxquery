<?xml version="1.0"?>
<!--
  Licensed to the Apache Software Foundation (ASF) under one or more
  contributor license agreements.  See the NOTICE file distributed with
  this work for additional information regarding copyright ownership.
  The ASF licenses this file to You under the Apache License, Version 2.0
  (the "License"); you may not use this file except in compliance with
  the License.  You may obtain a copy of the License at

      http://www.apache.org/licenses/LICENSE-2.0

  Unless required by applicable law or agreed to in writing, software
  distributed under the License is distributed on an "AS IS" BASIS,
  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
  See the License for the specific language governing permissions and
  limitations under the License.
-->
<xsl:transform xmlns:xsl="http://www.w3.org/1999/XSL/Transform" version="2.0">
    <xsl:output method="text" omit-xml-declaration="yes"/>

    <xsl:template match="/">
        <xsl:for-each select="/operators/operator">
            @SuppressWarnings("unchecked")
            public static final org.apache.vxquery.functions.Operator <xsl:value-of select="translate(substring-after(@name, ':'), 'abcdefghijklmnopqrstuvwxyz-:', 'ABCDEFGHIJKLMNOPQRSTUVWXYZ__')"/>
                = new org.apache.vxquery.functions.Operator(<xsl:value-of select="translate(substring-after(@name, ':'), 'abcdefghijklmnopqrstuvwxyz-:', 'ABCDEFGHIJKLMNOPQRSTUVWXYZ__')"/>_QNAME,
                new org.apache.vxquery.functions.Signature(
                createSequenceType("<xsl:value-of select="return/@type"/>")
                <xsl:if test="@varargs = 'true'">
                    , true
                </xsl:if>
                <xsl:for-each select="param">
                    ,
                    org.apache.commons.lang3.tuple.Pair.&lt;javax.xml.namespace.QName, org.apache.vxquery.types.SequenceType&gt;of(
                        new javax.xml.namespace.QName("<xsl:value-of select="@name"/>"),
                        createSequenceType("<xsl:value-of select="@type"/>")
                    )
                </xsl:for-each>
                )
                ) {
                <xsl:if test="property">
                    {
                    <xsl:for-each select="property">
                        <xsl:if test="@type = 'DocumentOrder'">
                            this.documentOrderPropagationPolicy = new <xsl:value-of select="@class"/>(
                            <xsl:for-each select="argument">
                               <xsl:value-of select="@value"/>
                               <xsl:if test="position() != last()">,</xsl:if>
                            </xsl:for-each>
                            );
                        </xsl:if>
                        <xsl:if test="@type = 'UniqueNodes'">
                            this.uniqueNodesPropagationPolicy = new <xsl:value-of select="@class"/>(
                            <xsl:for-each select="argument">
                               <xsl:value-of select="@value"/>
                               <xsl:if test="position() != last()">,</xsl:if>
                            </xsl:for-each>
                            );
                        </xsl:if>
                    </xsl:for-each>
                    }
                </xsl:if>
                <xsl:for-each select="runtime">
                    <xsl:if test="@type = 'scalar'">
                    {
                         this.scalarEvaluatorFactory = true;
                    }
                    public org.apache.hyracks.algebricks.runtime.base.IScalarEvaluatorFactory createScalarEvaluatorFactory(org.apache.hyracks.algebricks.runtime.base.IScalarEvaluatorFactory[] args) {
                        return new <xsl:value-of select="@class"/>(args);
                    }
                    </xsl:if>
                    <xsl:if test="@type = 'aggregate'">
                    {
                        this.aggregateEvaluatorFactory = true;
                    }
                    public org.apache.hyracks.algebricks.runtime.base.IAggregateEvaluatorFactory createAggregateEvaluatorFactory(org.apache.hyracks.algebricks.runtime.base.IScalarEvaluatorFactory[] args) {
                        return new <xsl:value-of select="@class"/>(args);
                    }
                    </xsl:if>
                    <xsl:if test="@type = 'unnesting'">
                    {
                        this.unnestingEvaluatorFactory = true;
                    }
                    public org.apache.hyracks.algebricks.runtime.base.IUnnestingEvaluatorFactory createUnnestingEvaluatorFactory(org.apache.hyracks.algebricks.runtime.base.IScalarEvaluatorFactory[] args) {
                        return new <xsl:value-of select="@class"/>(args);
                    }
                    </xsl:if>
                </xsl:for-each>
                };
        </xsl:for-each>
    </xsl:template>
</xsl:transform>
