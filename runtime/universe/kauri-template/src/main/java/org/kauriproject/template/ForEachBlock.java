/*
 * Copyright 2008 Outerthought bvba and Schaubroeck nv
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.kauriproject.template;

import java.util.Collection;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.kauriproject.template.el.ELFacade;
import org.kauriproject.template.el.Expression;
import org.xml.sax.Attributes;
import org.xml.sax.Locator;
import org.w3c.dom.Node;
import org.w3c.dom.NodeList;

/**
 * Implementation of a foreach template instruction.
 */
public class ForEachBlock extends TemplateBlock {

    protected static Log log = LogFactory.getLog(ForEachBlock.class);

    // FOREACH CONSTANTS
    public static final String BEGIN = "begin";
    public static final String END = "end";
    public static final String STEP = "step";
    public static final String VAR = "var";
    public static final String IN = "in";

    // EL
    protected ELFacade elFacade;

    public ForEachBlock(ELFacade elFacade, SaxElement saxElement) {
        super(saxElement);
        this.elFacade = elFacade;
    }

    protected Step buildStartStep(Locator locator, NamespacesHandle namespacesHandle) {
        return new StartStep(locator);
    }

    protected Step buildEndStep(Locator locator) {
        return new EndStep(locator);
    }

    class StartStep extends Step {

        // foreach stuff
        private final Expression beginExpr;
        private final Expression endExpr;
        private final Expression stepExpr;
        private final Expression varExpr;
        private final Expression inExpr;

        // compiledNext: first nested instruction
        // runtimeNext may be endStep.compiledNext

        public StartStep(Locator locator) {
            super(locator);
            final Attributes attributes = getSaxElement().getAttributes();

            final String beginAttr = attributes.getValue(BEGIN);
            final String endAttr = attributes.getValue(END);
            final String stepAttr = attributes.getValue(STEP);
            final String varAttr = attributes.getValue(VAR);
            final String inAttr = attributes.getValue(IN);

            beginExpr = elFacade.createExpression(beginAttr, String.class);
            endExpr =  elFacade.createExpression(endAttr, String.class);
            stepExpr = elFacade.createExpression(stepAttr, String.class);
            varExpr = elFacade.createExpression(varAttr, String.class);
            inExpr = elFacade.createExpression(inAttr, Object.class);
        }

        @Override
        public Step executeAndProceed(ExecutionContext context, TemplateResult result) {
            TemplateContext tmplContext = context.getTemplateContext();

            // begin
            final int begin = intEval(context, beginExpr, 0, BEGIN);

            // end
            final Integer end;
            if (endExpr != null) {
                end = parseInt((String) endExpr.evaluate(tmplContext), END);
            } else if (inExpr != null) {
                end = null;
            } else {
                end = 0;
            }

            // step
            final int step = intEval(context, stepExpr,  1, STEP);
            if (step <= 0) {
                log.error("Invalid value for step: " + step + ", must be > 0 .");
            }

            // var
            final String var = stringEval(context, varExpr, "item");

            Object[] array = null;
            // check if iteration over collection
            if (inExpr != null) {
                Object items = inExpr.evaluate(tmplContext);
                if (items == null) {
                    array = new Object[0];
                } else if (items.getClass().isArray()) {
                    array = (Object[]) items;
                } else if (items instanceof Collection) {
                    array = ((Collection)items).toArray();
                } else if (items instanceof Node) {
                    NodeList nodeList = ((Node)items).getChildNodes();
                    array = new Object[nodeList.getLength()];
                    for (int i = 0; i < nodeList.getLength(); i++)
                        array[i] = nodeList.item(i);
                } else if (items instanceof NodeList) {
                    NodeList nodeList = (NodeList)items;
                    array = new Object[nodeList.getLength()];
                    for (int i = 0; i < nodeList.getLength(); i++)
                        array[i] = nodeList.item(i);                    
                } else {
                    // create a single-entry list
                    array = new Object[] { items };
                }
            }

            // correct start- and end-value of loop
            int loopStart = begin;
            if (array != null && begin < 0) {
                loopStart = 0;
            }

            int loopEnd;
            if (end == null || (array != null && end > array.length - 1)) {
                loopEnd = array.length - 1;
            } else {
                loopEnd = end;
            }

            // execute step
            if (loopStart <= loopEnd && step > 0) {
                // create new loop status object
                LoopStatus status = new LoopStatus(loopStart, loopEnd, step, array, var);
                tmplContext.saveContext();
                tmplContext.put(LoopStatus.KEY, status);
                if (array != null) {
                    tmplContext.put(var, array[loopStart]);
                }
                return getCompiledNext();
            } else {
                return endStep.getCompiledNext();
            }
        }
    }

    private int parseInt(String value, String what) {
        try {
            return Integer.parseInt(value);
        } catch (NumberFormatException e) {
            throw new TemplateException("Value \"" + value + "\" is not a valid integer, in forEach attribute " + what + " at " + startStep.getLocation());
        }
    }

    public int intEval(final ExecutionContext context, final Expression expr, final int dfault, final String attrName) {
        return expr != null ? parseInt((String)expr.evaluate(context.getTemplateContext()), attrName) : dfault;
    }


    class EndStep extends Step {

        // compiledNext: first following instruction after block
        // runtimeNext may be startStep.compiledNext

        public EndStep(Locator locator) {
            super(locator);
        }

        @Override
        public Step executeAndProceed(ExecutionContext context, TemplateResult result) {
            TemplateContext tmplContext = context.getTemplateContext();
            LoopStatus status = (LoopStatus) tmplContext.get(LoopStatus.KEY);
            if (status == null) {
                log.error("Status should not be null in the foreach-EndStep.");
                // TODO: return ? cleanup ?
                return getCompiledNext();
            } else {
                if (status.increment()) {
                    tmplContext.put(LoopStatus.KEY, status);
                    if (status.getArray() != null) {
                        tmplContext.put(status.getVarName(), status.getArray()[status.getPosition()]);
                    }
                    return startStep.getCompiledNext();
                } else {
                    tmplContext.restoreContext();
                    return getCompiledNext();
                }
            }

        }

    }

    /**
     * First draft implementation as inner class.
     * 
     * Object should give access to:
     * <ul>
     * <li>index (starts from 0)</li>
     * <li>number (starts from 1)</li>
     * <li>letter (starts from A)</li>
     * <li>size of collection</li>
     * <li>revindex (reverse index)</li>
     * <li>revnumber (reverse number)</li>
     * <li>parentloop (for nested loop)</li>
     * </ul>
     */
    public class LoopStatus {

        public static final String KEY = "loopStatus";

        private Object[] array;
        private String varName;

        // private final int begin;
        private final int end;
        private final int step;
        private final int size;

        private int position; // current position (between begin and end)
        private int index; // 0-based
        private int number; // 1-based
        private int revIndex; // reverse index
        private int revNumber;// reverse number

        public LoopStatus(int begin, int end, int step, Object[] array, String varName) {
            this.array = array;
            this.varName = varName;

            // this.begin = begin;
            this.end = end;
            this.step = step;
            this.size = (int) Math.ceil(Math.max(end - begin + 1, 0) / step);

            this.position = begin;
            this.index = 0;
            this.number = 1;
            this.revIndex = this.size - 1;
            this.revNumber = this.size;
        }

        /**
         * Computes the next position and flags if the loop should be continued.
         */
        public boolean increment() {
            boolean continueLoop = false;
            position = position + step;
            if (position <= end) {
                this.index++;
                this.number++;
                this.revIndex--;
                this.revNumber--;
                continueLoop = true;
            }
            return continueLoop;
        }

        protected Object[] getArray() {
            return array;
        }

        protected String getVarName() {
            return varName;
        }

        // -----------------------

        public int getPosition() {
            return position;
        }

        public int getNumber() {
            return number;
        }

        public int getIndex() {
            return index;
        }

        public int getRevIndex() {
            return revIndex;
        }

        public int getRevNumber() {
            return revNumber;
        }

    }

}
