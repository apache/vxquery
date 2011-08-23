/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 * http://www.apache.org/licenses/LICENSE-2.0
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.vxquery.compiler.expression;

public class ExpressionPrinter {
    public static void prettyPrint(Expression e, int level, StringBuilder buffer) {
        e.accept(new PrettyPrinter(buffer, level));
    }

    public static String prettyPrint(Expression e) {
        StringBuilder buffer = new StringBuilder();
        prettyPrint(e, 0, buffer);
        return buffer.toString();
    }

    private static final class PrettyPrinter implements ExpressionVisitor<Void> {
        private StringBuilder buffer;
        private int level;

        public PrettyPrinter(StringBuilder buffer, int level) {
            this.buffer = buffer;
            this.level = level;
        }

        private StringBuilder indent(int level) {
            for (int i = 0; i < level; ++i) {
                buffer.append("   ");
            }
            return buffer;
        }

        private void print(ExpressionHandle e) {
            ++level;
            e.accept(this);
            --level;
        }

        private void print(String s) {
            buffer.append(s);
        }

        private void print(Variable v) {
            buffer.append("$").append(v.getName());
        }

        @Override
        public Void visitAttributeNodeConstructorExpression(AttributeNodeConstructorExpression expr) {
            indent(level);
            buffer.append("construct-attribute [\n");
            print(expr.getName());
            print(",\n");
            print(expr.getContent());
            print("\n");
            indent(level);
            print("]");
            return null;
        }

        @Override
        public Void visitCastExpression(CastExpression expr) {
            indent(level);
            buffer.append("cast [").append(expr.getType()).append("] [\n");
            print(expr.getInput());
            print("\n");
            indent(level);
            print("]");
            return null;
        }

        @Override
        public Void visitCastableExpression(CastableExpression expr) {
            indent(level);
            buffer.append("castable [").append(expr.getType()).append("] [\n");
            print(expr.getInput());
            print("\n");
            indent(level);
            print("]");
            return null;
        }

        @Override
        public Void visitCommentNodeConstructorExpression(CommentNodeConstructorExpression expr) {
            indent(level);
            buffer.append("construct-comment [\n");
            print(expr.getContent());
            print("\n");
            indent(level);
            print("]");
            return null;
        }

        @Override
        public Void visitConstantExpression(ConstantExpression expr) {
            indent(level);
            buffer.append("constant [").append(expr.getValue()).append("] [").append(expr.getType()).append("]");
            return null;
        }

        @Override
        public Void visitDocumentNodeConstructorExpression(DocumentNodeConstructorExpression expr) {
            indent(level);
            buffer.append("construct-document [\n");
            print(expr.getContent());
            print("\n");
            indent(level);
            print("]");
            return null;
        }

        @Override
        public Void visitElementNodeConstructorExpression(ElementNodeConstructorExpression expr) {
            indent(level);
            buffer.append("construct-element [\n");
            print(expr.getName());
            print(",\n");
            print(expr.getContent());
            print("\n");
            indent(level);
            print("]");
            return null;
        }

        @Override
        public Void visitExtensionExpression(ExtensionExpression expr) {
            return null;
        }

        @Override
        public Void visitFLWORExpression(FLWORExpression expr) {
            for (FLWORExpression.Clause clause : expr.getClauses()) {
                indent(level);
                switch (clause.getTag()) {
                    case FOR: {
                        FLWORExpression.ForClause fc = (FLWORExpression.ForClause) clause;
                        ForLetVariable fv = fc.getForVariable();
                        PositionVariable pv = fc.getPosVariable();
                        ScoreVariable sv = fc.getScoreVariable();
                        buffer.append("for [");
                        print(fv);
                        buffer.append(", ");
                        if (pv == null) {
                            print("<NULL>");
                        } else {
                            print(pv);
                        }
                        buffer.append(", ");
                        if (sv == null) {
                            print("<NULL>");
                        } else {
                            print(sv);
                        }
                        buffer.append("] ->\n");
                        print(fv.getSequence());
                        break;
                    }
                    case LET: {
                        FLWORExpression.LetClause lc = (FLWORExpression.LetClause) clause;
                        ForLetVariable lv = lc.getLetVariable();
                        buffer.append("let [");
                        print(lv);
                        buffer.append("] ->\n");
                        print(lv.getSequence());
                        break;
                    }
                    case ORDERBY:
                        break;
                    case WHERE: {
                        FLWORExpression.WhereClause wc = (FLWORExpression.WhereClause) clause;
                        buffer.append("where\n");
                        print(wc.getCondition());
                        break;
                    }
                }
                buffer.append("\n");
            }
            indent(level);
            buffer.append("return\n");
            print(expr.getReturnExpression());
            return null;
        }

        @Override
        public Void visitFunctionCallExpression(FunctionCallExpression expr) {
            indent(level);
            buffer.append("fn [").append(expr.getFunction().getName()).append('/')
                    .append(expr.getFunction().getSignature().getArity()).append("] [");
            for (ExpressionHandle arg : expr.getArguments()) {
                buffer.append("\n");
                print(arg);
                buffer.append(",");
            }
            buffer.append("\n");
            indent(level);
            buffer.append("]");
            return null;
        }

        @Override
        public Void visitIfThenElseExpression(IfThenElseExpression expr) {
            indent(level);
            buffer.append("if [\n");
            print(expr.getCondition());
            print("\n");
            indent(level);
            print("] then [\n");
            print(expr.getThenExpression());
            print("\n");
            indent(level);
            print("] else [\n");
            print(expr.getElseExpression());
            print("\n");
            indent(level);
            print("]");
            return null;
        }

        @Override
        public Void visitInstanceofExpression(InstanceofExpression expr) {
            indent(level);
            buffer.append("instance-of [").append(expr.getType()).append("] [\n");
            print(expr.getInput());
            print("\n");
            indent(level);
            print("]");
            return null;
        }

        @Override
        public Void visitPINodeConstructorExpression(PINodeConstructorExpression expr) {
            indent(level);
            buffer.append("construct-pi [\n");
            print(expr.getTarget());
            indent(level);
            print(",\n");
            print(expr.getContent());
            print("\n");
            indent(level);
            print("]");
            return null;
        }

        @Override
        public Void visitPathStepExpression(PathStepExpression expr) {
            indent(level);
            buffer.append("path-step [").append(expr.getAxis()).append("] [").append(expr.getNodeType())
                    .append("] [\n");
            print(expr.getInput());
            print("\n");
            indent(level);
            print("]");
            return null;
        }

        @Override
        public Void visitPromoteExpression(PromoteExpression expr) {
            indent(level);
            buffer.append("promote [").append(expr.getType()).append("] [\n");
            print(expr.getInput());
            print("\n");
            indent(level);
            print("]");
            return null;
        }

        @Override
        public Void visitQuantifiedExpression(QuantifiedExpression expr) {
            indent(level);
            buffer.append("quantified [").append(expr.getQuantification()).append("] [\n");
            ++level;
            for (ForLetVariable fv : expr.getQuantifiedVariables()) {
                indent(level);
                print(fv);
                buffer.append(" ->\n");
                print(fv.getSequence());
                buffer.append("\n");
            }
            --level;
            indent(level);
            buffer.append("] satisfies [\n");
            print(expr.getSatisfiesExpression());
            print("\n");
            indent(level);
            print("]");
            return null;
        }

        @Override
        public Void visitTextNodeConstructorExpression(TextNodeConstructorExpression expr) {
            indent(level);
            buffer.append("construct-text [\n");
            print(expr.getContent());
            print("\n");
            indent(level);
            print("]");
            return null;
        }

        @Override
        public Void visitTreatExpression(TreatExpression expr) {
            indent(level);
            buffer.append("treat [").append(expr.getType()).append("] [\n");
            print(expr.getInput());
            print("\n");
            indent(level);
            print("]");
            return null;
        }

        @Override
        public Void visitTypeswitchExpression(TypeswitchExpression expr) {
            return null;
        }

        @Override
        public Void visitValidateExpression(ValidateExpression expr) {
            indent(level);
            buffer.append("validate [").append(expr.getMode()).append("] [\n");
            print(expr.getInput());
            print("\n");
            indent(level);
            print("]");
            return null;
        }

        @Override
        public Void visitVariableReferenceExpression(VariableReferenceExpression expr) {
            indent(level);
            buffer.append("var-ref [ ");
            print(expr.getVariable());
            buffer.append(" ]");
            return null;
        }
    }
}