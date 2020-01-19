package rewriting;

import net.sf.jsqlparser.expression.Expression;
import net.sf.jsqlparser.expression.StringValue;
import net.sf.jsqlparser.expression.operators.conditional.AndExpression;
import net.sf.jsqlparser.expression.operators.relational.ComparisonOperator;
import net.sf.jsqlparser.expression.operators.relational.EqualsTo;
import net.sf.jsqlparser.schema.Column;
import net.sf.jsqlparser.util.deparser.ExpressionDeParser;

import java.util.ArrayList;

public class RelaxationAttributeSelectionFinder extends ExpressionDeParser {


    /**
     * List of all selection conditions on the relaxation attribute.
     */
    private ArrayList<EqualsTo> relaxationAttributeSelections;


// ############################ Construcotr #################################

    public RelaxationAttributeSelectionFinder() {
        this.relaxationAttributeSelections = new ArrayList<>();
    }


// ############################### Getter & Setter ##############################

    public ArrayList<EqualsTo> getRelaxationAttributeSelections() {
        return relaxationAttributeSelections;
    }


// ############################ Parsing Methods #################################


    /**
     * Check all the EqualsTo expressions whether they are selection conditions on the relxation attribute (e.g.
     * SELECT ... FROM ILL WHERE disease='Liver Failure'). Also transforms the expression in case of a selection
     * such that the left expression is always the column and the right one is the String literal.
     * @param equalsTo Possible selection condition
     */
    @Override
    public void visit(EqualsTo equalsTo) {
        super.visit(equalsTo);

        // Check if this is a selection condition on the relaxation attribute disease
        Expression left = equalsTo.getLeftExpression();
        Expression right = equalsTo.getRightExpression();
        Column column;

        if (left instanceof Column && right instanceof StringValue) {
            column = (Column) left;
            if (column.getColumnName().matches("(?i)disease"))
                this.relaxationAttributeSelections.add(equalsTo);
        } else if (right instanceof Column && left instanceof StringValue) {
            column = (Column) right;
            if (column.getColumnName().matches("(?i)disease")) {
                StringValue string = (StringValue) equalsTo.getLeftExpression();
                equalsTo.setLeftExpression(column);
                equalsTo.setRightExpression(string);
                this.relaxationAttributeSelections.add(equalsTo);
            }
        }
    }



    /**
     * Handle any ComparisonOperator expression.
     * @param comparisonOperator Comparison expression
     */
    public void visit(ComparisonOperator comparisonOperator) {
        // Accept only '='
        if (comparisonOperator instanceof EqualsTo) {
            this.visit((EqualsTo) comparisonOperator);
        }
    }





    /**
     * Entry point to the deparser. Takes a WHERE expression and deparses it and tries to find selection conditions
     * on the relaxation attribute 'disease' of relation 'ILL'.
     * NOTE: May produce undefined behavior if provided Expression is not a WHERE expression
     * @param where WHERE expression
     * @return True if selections were found; otherwise false
     * @throws UnsupportedExpressionException If any unsupported expression occurs while deparsing, this exception is
     *      thrown.
     */
    public boolean findRelaxationAttributeSelections (Expression where) throws UnsupportedExpressionException {

        if (where == null)
            return false;

        // Only accept if it is an And- or ComparisonExpressions
        if (where instanceof AndExpression) {
            AndExpression andExpression = (AndExpression) where;
            super.visit(andExpression);
        } else if (where instanceof ComparisonOperator) {
            ComparisonOperator comparisonOperator = (ComparisonOperator) where;
            this.visit(comparisonOperator);
        } else
            throw new UnsupportedExpressionException("WHERE Expression '" + where + "' is a not supported " +
                    "Expression (" + where.getClass() + ")!");

        return !(this.relaxationAttributeSelections.isEmpty());
    }

}
