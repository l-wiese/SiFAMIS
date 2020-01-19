package rewriting;

import net.sf.jsqlparser.expression.Alias;
import net.sf.jsqlparser.expression.Expression;
import net.sf.jsqlparser.expression.Function;
import net.sf.jsqlparser.expression.operators.relational.ExpressionList;
import net.sf.jsqlparser.expression.operators.relational.NamedExpressionList;
import net.sf.jsqlparser.schema.Column;
import net.sf.jsqlparser.schema.Table;
import net.sf.jsqlparser.statement.select.*;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

public class SelectClauseRewriter extends SelectItemVisitorAdapter {


    private int fragID;

    private List<SelectItem> rewrittenItems;

    private boolean allowOverwritingFragments;

    private String illRegex;

    private String infoRegex;

    private String treatRegex;

    /**
     * Constructor for Select clause rewriter for the given fragment id.
     *
     * @param fragID ID of the fragment
     */
    private SelectClauseRewriter(int fragID, boolean allowOverwritingFragments) {
        this.fragID = fragID;
        this.rewrittenItems = new ArrayList<>();
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
     * Does not need to be rewritten but just added as new Object to the list of rewritten items.
     * Note: This method is only public because it overwrites a method from the super class
     * {@link SelectItemVisitorAdapter}
     *
     * @param columns Asterisk '*'
     */
    @Override
    public void visit(AllColumns columns) {
        super.visit(columns);
        // Select * From ... does not need to be rewritten!
        rewrittenItems.add(new AllColumns());
    }


    /**
     * Rewrite AllTableColumns items (Select x.* From SomeTab x ...) if needed
     * Note: This method is only public because it overwrites a method from the super class
     * {@link SelectItemVisitorAdapter}
     *
     * @param columns An AllTableColumns item
     */
    @Override
    public void visit(AllTableColumns columns) {
        super.visit(columns);
        AllTableColumns rewritten;
        // Select x.* FROM SomeTab x, replace only if it is relation "ILL" (if aliased, rewriting in from is sufficient)
        if (columns.getTable().getName().matches(this.illRegex)) {

            // Ill fragment
            Table rewriteTable = new Table("ILL_" + this.fragID);
            rewritten = new AllTableColumns(rewriteTable);

        } else if (columns.getTable().getName().matches(this.infoRegex)) {

            // Info fragment
            Table rewriteTable = new Table("INFO_" + this.fragID);
            rewritten = new AllTableColumns(rewriteTable);

        } else if (columns.getTable().getName().matches(this.treatRegex)) {

            // Treat fragment
            Table rewriteTable = new Table("TREAT_" + this.fragID);
            rewritten = new AllTableColumns(rewriteTable);

        } else {
            Table t = new Table(columns.getTable().getName());
            Alias alias = columns.getTable().getAlias();
            if (alias != null)
                t.setAlias(new Alias(alias.getName(), alias.isUseAs()));
            rewritten = new AllTableColumns(t);
        }
        this.rewrittenItems.add(rewritten);
    }


    /**
     * Rewrite SelectExpressionItem items (Select X.abc ... From X ...) if needed.
     * Note: This method is only public because it overwrites a method from the super class
     * {@link SelectItemVisitorAdapter}
     *
     * @param item An SelectExpressionItem item
     */
    @Override
    public void visit(SelectExpressionItem item) {
        super.visit(item);

        // Select X.abc ... From ...
        SelectExpressionItem rewritten;
        Expression expr = item.getExpression();
        if (expr instanceof Column) {
            // Rewrite if matches
            Column c = matchColumn((Column) expr);
            rewritten = new SelectExpressionItem(c);

        } else if (expr instanceof Function) {
            // some Aggregation maybe that can contain a column expression
            rewritten = this.visit((Function) expr);

            // Any not supported expression causes an exception
        } else throw new UnsupportedExpressionException("An Error occured! The SelectExpressionItem " + item +
                " is of an unsupported expression type: " + expr.getClass() + "\n" +
                "Note: Column in aggregate functions should be aliased like 'Select sum(p.age) From INFO p', the " +
                "name of the table alone may produce strange behavior, e.g 'Select sum(INFO.age) From INFO'!");

        // Finally, add the rewritten (new) SelectExpressionItem
        this.rewrittenItems.add(rewritten);
    }


    /**
     * Rewrite column expressions occuring in a function
     *
     * @param function Function, e.g. sum(...) or count(...)
     * @return Rewritten SelectExpressionItem
     * @throws UnsupportedExpressionException
     */
    private SelectExpressionItem visit(Function function) throws UnsupportedExpressionException {

        if (function.isAllColumns()) {
            return new SelectExpressionItem(function);
        }

        // Else, create a new Function
        Function rewrittenFunc = new Function();
        rewrittenFunc.setName(function.getName());
        rewrittenFunc.setAttribute(function.getAttribute());
        rewrittenFunc.setDistinct(function.isDistinct());
        rewrittenFunc.setEscaped(function.isEscaped());

        // Get the parameters and check if that are expressions that need to be rewritten
        ExpressionList exprList = function.getParameters();
        if (exprList != null) {
            List<Expression> params = exprList.getExpressions();
            ExpressionList rewrittenParams = new ExpressionList(this.visit(params));
            rewrittenFunc.setParameters(rewrittenParams);
        }
        // Get all named parameters and do the same
        NamedExpressionList namedExprList = function.getNamedParameters();
        if (namedExprList != null) {
            List<Expression> namedParams = namedExprList.getExpressions();
            NamedExpressionList rewrittenNamedParams = new NamedExpressionList(this.visit(namedParams));
            rewrittenFunc.setNamedParameters(rewrittenNamedParams);
        }

        // Finally, return rewritten function
        return new SelectExpressionItem(rewrittenFunc);
    }


    /**
     * If the table corresponding to the given Column matches the "ILL" relation, then rewrite it!
     *
     * @param column Column
     * @return Rewritten Column
     */
    private Column matchColumn(Column column) {
        Column rewritten = null;
        if (column.getTable().getName().matches(this.illRegex)) {

            // Ill fragment
            Table rewriteTable = new Table("ILL_" + this.fragID);
            rewritten = new Column(rewriteTable, column.getColumnName());

        } else if (column.getTable().getName().matches(this.infoRegex)) {

            // Info fragment
            Table rewriteTable = new Table("INFO_" + this.fragID);
            rewritten = new Column(rewriteTable, column.getColumnName());

        } else if (column.getTable().getName().matches(this.treatRegex)) {

            // Treat fragment
            Table rewriteTable = new Table("TREAT_" + this.fragID);
            rewritten = new Column(rewriteTable, column.getColumnName());

        } else {
            String tabName = (column.getTable().getName());
            String columnName = (column.getColumnName());
            Table tab = new Table(tabName);
            rewritten = new Column(tab, columnName);
        }
        return rewritten;
    }


    /**
     * Visit accordingly to the type, throw an exception if type is invalid.
     *
     * @param item Item from select clause
     */
    private void visit(SelectItem item) {

        // Choose the right 'visit'-method
        if (item instanceof SelectExpressionItem) {
            this.visit((SelectExpressionItem) item);
        } else if (item instanceof AllColumns) {
            this.visit((AllColumns) item);
        } else if (item instanceof AllTableColumns) {
            this.visit((AllTableColumns) item);
        } else {
            throw new UnsupportedExpressionException("An Error occured! The SelectItem " + item + " is of an " +
                    "unsupported expression type!");
        }
    }


    /**
     * Rewrite a list of expressions --> only Column expressions are supported, anything else is rejected
     *
     * @param list List of expressions
     * @return Rewritten expression list
     * @throws UnsupportedExpressionException Thrown when an unsupported expression is in the list
     */
    private List<Expression> visit(List<Expression> list) throws UnsupportedExpressionException {
        List<Expression> rewrittenList = new ArrayList<>();
        for (Expression expression : list) {
            if (expression instanceof Column) {
                Column c = matchColumn((Column) expression);
                rewrittenList.add(c);
            } else
                throw new UnsupportedExpressionException("An Error occured! The Expression List " +
                        Arrays.toString(list.toArray()) +
                        " contains expressions of an unsupported expression type! " + expression.getClass());
        }
        return rewrittenList;
    }


    /**
     * Entry point. Takes a query body and rewrites the select clause of it according to the given fragment id of the
     * "ILL"-relation.
     *
     * @param body   Query body
     * @param fragID Fragment ID
     * @return Rewritten Select item list
     */
    public static List<SelectItem> rewrite(PlainSelect body, int fragID, boolean allowOverwritingFragments) {
        SelectClauseRewriter rewriter = new SelectClauseRewriter(fragID, allowOverwritingFragments);
        for (SelectItem item : body.getSelectItems()) {
            rewriter.visit(item);
        }
        return rewriter.rewrittenItems;
    }

}
