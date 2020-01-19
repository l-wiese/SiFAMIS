package rewriting;

import net.sf.jsqlparser.JSQLParserException;
import net.sf.jsqlparser.expression.Alias;
import net.sf.jsqlparser.expression.Expression;
import net.sf.jsqlparser.parser.CCJSqlParserUtil;
import net.sf.jsqlparser.schema.Column;
import net.sf.jsqlparser.schema.Table;
import net.sf.jsqlparser.statement.Statement;
import net.sf.jsqlparser.statement.select.PlainSelect;
import net.sf.jsqlparser.statement.select.Select;
import net.sf.jsqlparser.util.deparser.ExpressionDeParser;

import java.util.ArrayList;
import java.util.List;

public class GroupByRewriter extends ExpressionDeParser {

    private int fragID;
    private boolean allowOverwritingFragments;
    private String illRegex;
    private String infoRegex;
    private String treatRegex;
    private List<Expression> rewrittenColumns;


    public GroupByRewriter(int fragID, boolean allowOverwritingFragments) {
        this.fragID = fragID;
        this.allowOverwritingFragments = allowOverwritingFragments;
        this.illRegex = "(?i)ill";
        this.infoRegex = "(?i)info";
        this.treatRegex = "(?i)treat";
        if (allowOverwritingFragments) {
            this.illRegex += "(_\\d+)?";    // optionally matches fragments to overwrite
            this.infoRegex += "(_\\d+)?";
            this.treatRegex += "(_\\d+)?";
        }
        this.rewrittenColumns = new ArrayList<>();
    }

    @Override
    public void visit(Column tableColumn) {
        super.visit(tableColumn);
        Column rewrittenColumn;
        Table table = tableColumn.getTable();
        if (table != null && table.getName().matches(this.illRegex)) {

            // Ill fragment
            Table rewriteTable = new Table("ILL_" + fragID);
            Alias alias = tableColumn.getTable().getAlias();
            if (alias != null)
                rewriteTable.setAlias(alias);
            rewrittenColumn = new Column(rewriteTable, tableColumn.getColumnName());

        } else if (table != null && table.getName().matches(this.infoRegex)) {

            // Info fragment
            Table rewriteTable = new Table("INFO_" + fragID);
            Alias alias = tableColumn.getTable().getAlias();
            if (alias != null)
                rewriteTable.setAlias(alias);
            rewrittenColumn = new Column(rewriteTable, tableColumn.getColumnName());

        } else if (table != null && table.getName().matches(this.treatRegex)) {

            // Treat fragment
            Table rewriteTable = new Table("TREAT_" + fragID);
            Alias alias = tableColumn.getTable().getAlias();
            if (alias != null)
                rewriteTable.setAlias(alias);
            rewrittenColumn = new Column(rewriteTable, tableColumn.getColumnName());

        } else if (table != null) {
            // just copy the column reference - should not occur
            Table copy = new Table(tableColumn.getTable().getName());
            Alias alias = tableColumn.getTable().getAlias();
            if (alias != null)
                copy.setAlias(alias);
            rewrittenColumn = new Column(copy, tableColumn.getColumnName());
        } else {
            rewrittenColumn = new Column(tableColumn.getColumnName());
        }
        rewrittenColumns.add(rewrittenColumn);
    }


    /**
     * Rewrites the group by clause according to the given fragment ID.
     * @param body Select query body
     * @param fragID Fragment ID
     * @param allowOverwritingFragments Whether already rewritten fragments shall be overwritten or not
     * @return List of rewritten column references
     */
    public static List<Expression> rewrite(PlainSelect body, int fragID, boolean allowOverwritingFragments) {
        GroupByRewriter rewriter = new GroupByRewriter(fragID, allowOverwritingFragments);
        List<Expression> groupByColumns = body.getGroupByColumnReferences();
        if (groupByColumns == null)
            return null;

        // All group by expressions should be columns!
        for (Expression e : groupByColumns) {
            if (e instanceof Column)
                rewriter.visit((Column) e);
            else
                throw new UnsupportedExpressionException("The type of Expression '" + e + "' of the group by clause " +
                        "of the plain select '" + body + "' is not of type column but of type " + e.getClass());
        }

        return rewriter.rewrittenColumns;
    }


    /**
     * Test unit.
     * @param args Not used
     * @throws JSQLParserException Thrown in case of parsing error
     */
    public static void main(String[] args) throws JSQLParserException {
        Statement stmt = CCJSqlParserUtil.parse("SELECT * FROM ILL WHERE DISEASE='Liver Failure' GROUP BY DISEASE");
        Select select = (Select) stmt;
        PlainSelect body = (PlainSelect) select.getSelectBody();
        List<Expression> list = rewrite(body, 0, false);
        body.setGroupByColumnReferences(list);
        System.out.println("Rewritten: " + body);

        stmt = CCJSqlParserUtil.parse("SELECT ILL.DISEASE, p.AGE, t.PRESCRIPTION FROM ILL, INFO p, TREAT t " +
                "GROUP BY ILL.disease, p.AGE, t.PRESCRIPTION");
        select = (Select) stmt;
        body = (PlainSelect) select.getSelectBody();
        list = rewrite(body, 0,true);
        body.setGroupByColumnReferences(list);
        System.out.println("Rewritten: " + body);
    }

}
