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
package org.apache.vxquery.datamodel.accessors;

import org.apache.vxquery.datamodel.accessors.atomic.XSBinaryPointable;
import org.apache.vxquery.datamodel.accessors.atomic.XSDatePointable;
import org.apache.vxquery.datamodel.accessors.atomic.XSDateTimePointable;
import org.apache.vxquery.datamodel.accessors.atomic.XSDecimalPointable;
import org.apache.vxquery.datamodel.accessors.atomic.XSDurationPointable;
import org.apache.vxquery.datamodel.accessors.atomic.XSQNamePointable;
import org.apache.vxquery.datamodel.accessors.atomic.XSTimePointable;
import org.apache.vxquery.datamodel.accessors.jsonitem.ArrayPointable;
import org.apache.vxquery.datamodel.accessors.jsonitem.ObjectPointable;
import org.apache.vxquery.datamodel.accessors.nodes.AttributeNodePointable;
import org.apache.vxquery.datamodel.accessors.nodes.DocumentNodePointable;
import org.apache.vxquery.datamodel.accessors.nodes.ElementNodePointable;
import org.apache.vxquery.datamodel.accessors.nodes.NodeTreePointable;
import org.apache.vxquery.datamodel.accessors.nodes.PINodePointable;
import org.apache.vxquery.datamodel.accessors.nodes.TextOrCommentNodePointable;

import org.apache.hyracks.data.std.primitive.BooleanPointable;
import org.apache.hyracks.data.std.primitive.BytePointable;
import org.apache.hyracks.data.std.primitive.DoublePointable;
import org.apache.hyracks.data.std.primitive.FloatPointable;
import org.apache.hyracks.data.std.primitive.IntegerPointable;
import org.apache.hyracks.data.std.primitive.LongPointable;
import org.apache.hyracks.data.std.primitive.ShortPointable;
import org.apache.hyracks.data.std.primitive.UTF8StringPointable;

/**
 * One pointable for each type.
 * 
 * Pointable group good for fixed single value case that needs all possible pointable types. 
 * Nice alternative to pool if the size does not ever change.
 */
public class TypedPointables {
    // Native Hyracks
    public BooleanPointable boolp = (BooleanPointable) BooleanPointable.FACTORY.createPointable();
    public BytePointable bytep = (BytePointable) BytePointable.FACTORY.createPointable();
    public DoublePointable doublep = (DoublePointable) DoublePointable.FACTORY.createPointable();
    public FloatPointable floatp = (FloatPointable) FloatPointable.FACTORY.createPointable();
    public IntegerPointable intp = (IntegerPointable) IntegerPointable.FACTORY.createPointable();
    public LongPointable longp = (LongPointable) LongPointable.FACTORY.createPointable();
    public ShortPointable shortp = (ShortPointable) ShortPointable.FACTORY.createPointable();
    public SequencePointable seqp = (SequencePointable) SequencePointable.FACTORY.createPointable();
    public UTF8StringPointable utf8sp = (UTF8StringPointable) UTF8StringPointable.FACTORY.createPointable();

    // XQuery Specific
    public XSBinaryPointable binaryp = (XSBinaryPointable) XSBinaryPointable.FACTORY.createPointable();
    public XSDatePointable datep = (XSDatePointable) XSDatePointable.FACTORY.createPointable();
    public XSDateTimePointable datetimep = (XSDateTimePointable) XSDateTimePointable.FACTORY.createPointable();
    public XSDecimalPointable decp = (XSDecimalPointable) XSDecimalPointable.FACTORY.createPointable();
    public XSDurationPointable durationp = (XSDurationPointable) XSDurationPointable.FACTORY.createPointable();
    public XSTimePointable timep = (XSTimePointable) XSTimePointable.FACTORY.createPointable();
    public XSQNamePointable qnamep = (XSQNamePointable) XSQNamePointable.FACTORY.createPointable();
    public ArrayPointable ap = (ArrayPointable) ArrayPointable.FACTORY.createPointable();
    public ObjectPointable op = (ObjectPointable) ObjectPointable.FACTORY.createPointable();

    // XQuery Nodes
    public AttributeNodePointable anp = (AttributeNodePointable) AttributeNodePointable.FACTORY.createPointable();
    public DocumentNodePointable dnp = (DocumentNodePointable) DocumentNodePointable.FACTORY.createPointable();
    public ElementNodePointable enp = (ElementNodePointable) ElementNodePointable.FACTORY.createPointable();
    public NodeTreePointable ntp = (NodeTreePointable) NodeTreePointable.FACTORY.createPointable();
    public PINodePointable pinp = (PINodePointable) PINodePointable.FACTORY.createPointable();
    public TextOrCommentNodePointable tocnp = (TextOrCommentNodePointable) TextOrCommentNodePointable.FACTORY
            .createPointable();
}
