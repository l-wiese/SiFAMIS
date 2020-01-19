package partitionnumbers;

import net.sf.jsqlparser.JSQLParserException;
import net.sf.jsqlparser.expression.Expression;
import net.sf.jsqlparser.expression.StringValue;
import net.sf.jsqlparser.expression.operators.conditional.AndExpression;
import net.sf.jsqlparser.expression.operators.relational.ComparisonOperator;
import net.sf.jsqlparser.expression.operators.relational.EqualsTo;
import net.sf.jsqlparser.parser.CCJSqlParserUtil;
import net.sf.jsqlparser.schema.Column;
import net.sf.jsqlparser.statement.select.PlainSelect;
import net.sf.jsqlparser.statement.select.Select;
import net.sf.jsqlparser.util.deparser.ExpressionDeParser;
import rewriting.UnsupportedExpressionException;

import java.util.ArrayList;

/**
 * This class implements the generalization of the relaxation attribute selections
 * in the SQL query.
 */
public class RelaxationSelectionGeneralizer extends ExpressionDeParser {


    /**
     * Stores all expressions that are not generalized.
     */
    protected ArrayList<Expression> expressions;

    /**
     * Constructor
     */
    public RelaxationSelectionGeneralizer() {
        this.expressions = new ArrayList<>();
    }

    /**
     * Detect if this is a selection condition, and if so, then generalize it by omitting it and the query must then
     * be executed at the corresponding partition (cluster) locally.
     *
     * @param equalsTo Selection condition
     */
    @Override
    public void visit(EqualsTo equalsTo) {
        // Check if this is a selection condition on the relaxation attribute disease
        Expression left = equalsTo.getLeftExpression();
        Expression right = equalsTo.getRightExpression();
        Column column;

        if (left instanceof Column && right instanceof StringValue) {
            column = (Column) left;
            if (!column.getColumnName().matches("(?i)disease")) {       // else is omitted
                this.expressions.add(equalsTo);
            }
        } else if (right instanceof Column && left instanceof StringValue) {
            column = (Column) right;
            if (!column.getColumnName().matches("(?i)disease")) {       // else is omitted
                this.expressions.add(equalsTo);
            }
        } else {
            this.expressions.add(equalsTo);
        }
    }

    /**
     * Handle any ComparisonOperator expression. Explore only EqualsTo-Selections (e.g. disease = '...') others are
     * unchanged
     *
     * @param comparisonOperator Comparison expression
     */
    public void visit(ComparisonOperator comparisonOperator) {
        // Accept only '='
        if (comparisonOperator instanceof EqualsTo) {
            this.visit((EqualsTo) comparisonOperator);
        } else {
            this.expressions.add(comparisonOperator);
        }
    }


    /**
     * Process And-Expression.
     *
     * @param andExpression AndExpression to process
     */
    @Override
    public void visit(AndExpression andExpression) {
        Expression left = andExpression.getLeftExpression();
        this.visit(left);
        Expression right = andExpression.getRightExpression();
        this.visit(right);
    }

    public void visit(Expression expression) {
        // Only accept if it is an And- or ComparisonExpressions (only conjunctive formulas)
        if (expression instanceof AndExpression) {
            this.visit((AndExpression) expression);
        } else if (expression instanceof ComparisonOperator) {
            this.visit((ComparisonOperator) expression);
        } else
            throw new UnsupportedExpressionException("Sub Expression '" + expression + "' is a not supported " +
                    "Expression (" + expression.getClass() + ")!");
    }


    /**
     * Build a new conjunctive where expression recursively.
     *
     * @return Generalized Where Expression
     */
    protected Expression buildWhereExpression() {

        // Check if only one or even none condition present
        int numExpressions = this.expressions.size();
        if (numExpressions < 1) {
            return null;
        } else if (numExpressions == 1) {
            return this.expressions.remove(0);
        }

        // Build an and expression recursively
        Expression left = this.expressions.remove(0);
        Expression right = buildWhereExpression();
        return new AndExpression(left, right);
    }


    /**
     * Generalize the selections in the given SQL query
     *
     * @param sql SQL query
     * @return Generalized query
     * @throws JSQLParserException Error when parsing and generalizing the query
     */
    public static String generalizeSelections(String sql) throws JSQLParserException {

        RelaxationSelectionGeneralizer generalizer = new RelaxationSelectionGeneralizer();

        // Parse sql
        Select stmt = (Select) CCJSqlParserUtil.parse(sql);
        PlainSelect body = (PlainSelect) stmt.getSelectBody();
        Expression where = body.getWhere();

        // Generalize where
        generalizer.visit(where);
        Expression newWhere = generalizer.buildWhereExpression();
        body.setWhere(newWhere);
        return body.toString();
    }


    /**
     * Test unit
     *
     * @param args Not used
     * @throws JSQLParserException Exception upon parsing
     */
    public static void main(String[] args) throws JSQLParserException {
        String sql = "SELECT * FROM ILL WHERE disease = 'Cough'";
        System.out.println(generalizeSelections(sql));

        sql = "SELECT * FROM ILL i, INFO p WHERE i.disease = 'Cough' and p.id = i.id and p.age > 20";
        System.out.println(generalizeSelections(sql));
    }

}
