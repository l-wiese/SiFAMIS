package rewriting;

import net.sf.jsqlparser.JSQLParserException;
import net.sf.jsqlparser.expression.Alias;
import net.sf.jsqlparser.expression.Expression;
import net.sf.jsqlparser.expression.operators.conditional.AndExpression;
import net.sf.jsqlparser.expression.operators.relational.ComparisonOperator;
import net.sf.jsqlparser.parser.CCJSqlParserUtil;
import net.sf.jsqlparser.schema.Column;
import net.sf.jsqlparser.schema.Table;
import net.sf.jsqlparser.statement.select.PlainSelect;
import net.sf.jsqlparser.util.deparser.ExpressionDeParser;

/**
 * Rewrite a Where clause by deparsing it and substituting any occurence of the relation 'ILL' by the corresponding
 * materialized fragment (i.e. 'ILL_fragID').
 * NOTE: The modification of the given query body is in-place!
 */
public class WhereClauseRewriter extends ExpressionDeParser {


    private int fragID;

    private boolean allowOverwritingFragments;

    private String illRegex;

    private String infoRegex;

    private String treatRegex;

    /**
     * Constructor for Where clause rewriter for the given fragment id.
     * @param fragID Fragment id
     */
    private WhereClauseRewriter(int fragID, boolean allowOverwritingFragments) {
        super();
        this.fragID = fragID;
        this.allowOverwritingFragments = allowOverwritingFragments;
        this.illRegex = "(?i)ill";
        this.infoRegex = "(?i)info";
        this.treatRegex = "(?i)treat";
        if (allowOverwritingFragments) {
            illRegex += "(_\\d+)?";    // optionally matches fragments to overwrite
            infoRegex += "(_\\d+)?";
            treatRegex += "(_\\d+)?";
        }
    }


    /**
     * Rewrite any Column expression accordingly.
     * Note: This method is only public because it overwrites a method from the super class
     *      {@link RelaxationAttributeSelectionFinder}
     */
    @Override
    public void visit(Column tableColumn) {
        super.visit(tableColumn);
        if (tableColumn.getTable().getName().matches(this.illRegex)) {
            Table rewriteTable = new Table("ILL_"+fragID);
            Alias alias = tableColumn.getTable().getAlias();
            if (alias != null)
                rewriteTable.setAlias(alias);
            tableColumn.setTable(rewriteTable);
        } else if (tableColumn.getTable().getName().matches(this.infoRegex)) {
            Table rewriteTable = new Table("INFO_"+fragID);
            Alias alias = tableColumn.getTable().getAlias();
            if (alias != null)
                rewriteTable.setAlias(alias);
            tableColumn.setTable(rewriteTable);
        } else if (tableColumn.getTable().getName().matches(this.treatRegex)) {
            Table rewriteTable = new Table("TREAT_"+fragID);
            Alias alias = tableColumn.getTable().getAlias();
            if (alias != null)
                rewriteTable.setAlias(alias);
            tableColumn.setTable(rewriteTable);
        }
    }


    /**
     * Handle any ComparisonOperator expression.
     * @param comparisonOperator Comparison expression
     */
    public void visit(ComparisonOperator comparisonOperator) {
        Expression left = comparisonOperator.getLeftExpression();
        Expression right = comparisonOperator.getRightExpression();

        if (left instanceof Column)
            this.visit((Column) left);
        if (right instanceof Column)
            this.visit((Column) right);
    }



    /**
     * Entry point. Takes a query body and rewrites it according to the fragment id of the "ILL"-relation.
     * @param body Query body
     * @param fragID Fragment ID of the "ILL"-relation.
     * @return Rewritten where expression
     * @throws UnsupportedExpressionException Thrown if any unsupported expression/type occurs while rewriting.
     * @throws JSQLParserException Exception upon parsing
     */
    public static Expression rewrite(PlainSelect body, int fragID, boolean allowOverwritingFragments)
            throws UnsupportedExpressionException, JSQLParserException {

        WhereClauseRewriter rewriter = new WhereClauseRewriter(fragID, allowOverwritingFragments);
        Expression where = body.getWhere();

        // No where present?
        if (where == null)
            return null;

        Expression whereCopy = CCJSqlParserUtil.parseCondExpression(where.toString());

        // Only accept if it is an And- or ComparisonExpressions (only conjunctive formulas)
        if (whereCopy instanceof AndExpression) {
            AndExpression andExpression = (AndExpression) whereCopy;
            rewriter.visit(andExpression);
        } else if (where instanceof ComparisonOperator) {
            ComparisonOperator comparisonOperator = (ComparisonOperator) whereCopy;
            rewriter.visit(comparisonOperator);
        } else
            throw new UnsupportedExpressionException("WHERE Expression '" + whereCopy + "' is a not supported " +
                    "Expression (" + where.getClass() + ")!");

        return whereCopy;
    }


}
