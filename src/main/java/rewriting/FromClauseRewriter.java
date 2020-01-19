package rewriting;

import net.sf.jsqlparser.expression.Alias;
import net.sf.jsqlparser.schema.Table;
import net.sf.jsqlparser.statement.select.*;
import org.apache.commons.lang3.tuple.ImmutablePair;
import org.apache.commons.lang3.tuple.Pair;

import java.util.ArrayList;
import java.util.List;
import java.util.Set;

/**
 * Rewrite a From clause by deparsing it and substituting any occurence of the relation 'ILL' by the corresponding
 * materialized fragment table by avoiding ignite's naming errors on tables (if table/cache is created via api which is
 * necessary here with a own affinity function, the table will be named Schemaname.CacheObject) by using the actual
 * fragment table as schema (i.e. "ILL_fragID.ILL"). As cross-schema queries work, only the rewriting gets more complex.
 * NOTE: The modification of the given query body is not in-place and syntactic sugar with 'JOIN ... ON' etc. is not
 * supported, only simple joins like 'FROM ILL i, INFO p' are supported (join condition must be in where clause).
 */
public class FromClauseRewriter implements FromItemVisitor {

    private Select select;

    private int fragID;

    private String instance;

    private List<Table> rewrittenTables;

    private final String aliasPrefix = "t";

    private int aliasCounter;

    private List<Table> tables;

    private Set<String> aliases;

    private boolean allowOverwritingFragments;

    private String illRegex;

    private String infoRegex;

    private String treatRegex;

// ################################## Constructors #########################################

    /**
     * Constructor for From clause rewriter for the given fragment id and the relation instance that has to be
     * considered for rewriting.
     * @param select Select statement
     * @param fragID ID of the fragment
     * @param instance Relation instance
     * @param allowOverwritingFragments Whether fragment expressions ("TAB_2" for fragment 2 of table TAB) are
     *                                  overwritten
     */
    private FromClauseRewriter(Select select, int fragID, boolean allowOverwritingFragments, String instance) {
        super();
        this.select = select;
        this.fragID = fragID;
        this.instance = instance;
        this.rewrittenTables = new ArrayList<>();
        this.aliasCounter = 0;

        this.allowOverwritingFragments = allowOverwritingFragments;
        this.illRegex = "(?i)ill";
        this.infoRegex = "(?i)info";
        this.treatRegex = "(?i)treat";
        if (allowOverwritingFragments) {
            this.illRegex += "(_\\d+)?";    // optionally matches fragments to overwrite
            this.infoRegex += "(_\\d+)?";
            this.treatRegex += "(_\\d+)?";
        }
    }


// ################################## Overwritten Methods ##################################

    /**
     * Rewrite any table name according to the fragment id of the corresponding seperate table fragment of the relation
     * "ILL".
     * @param table Table
     */
    @Override
    public void visit(Table table) {

        // Only rewrite ILL relation
        Table rewrittenTab = null;
        if (table.getName().matches(this.illRegex)) {

            // Check if relation instance has to be considered
            Alias alias = table.getAlias();
            if (this.instance == null) {

                // instance doesn't have to be considered, alias should not be forgotten
                rewrittenTab = new Table("ILL_" + this.fragID);
                if (alias != null)
                    rewrittenTab.setAlias(new Alias(alias.getName(), alias.isUseAs()));

            } else {
                // If the alias matches the given instance, rewrite it; if the alias does not match, no need for
                // rewriting; if there is no alias --> give it an alias
                if (alias != null && alias.getName().equals(this.instance)) {
                    rewrittenTab = new Table("ILL_" + this.fragID);
                    rewrittenTab.setAlias(new Alias(this.instance, alias.isUseAs()));
                } else {
                    // NO REWRITING --> wrong table instance
                    rewrittenTab = this.copyTable(table);
                }
            }

        } else if (table.getName().matches(this.infoRegex)) {

            // Check if relation instance has to be considered
            Alias alias = table.getAlias();

            // instance doesn't have to be considered here, but alias should not be forgotten
            rewrittenTab = new Table("INFO_" + this.fragID);
            if (alias != null)
                rewrittenTab.setAlias(new Alias(alias.getName(), alias.isUseAs()));


        } else if (table.getName().matches(this.treatRegex)) {

            // Check if relation instance has to be considered
            Alias alias = table.getAlias();

            // instance doesn't have to be considered, alias should not be forgotten
            rewrittenTab = new Table("TREAT_" + this.fragID);
            if (alias != null)
                rewrittenTab.setAlias(new Alias(alias.getName(), alias.isUseAs()));

        } else {
            // NO REWRITING
            rewrittenTab = copyTable(table);
        }

        this.rewrittenTables.add(rewrittenTab);
    }

    @Override
    public void visit(SubJoin subjoin) {

        // Visit left and all right from items (all are joined simply, i.e. 'FROM TabA a, TabB b, TabC c')
        this.visit(subjoin.getLeft());
        List<Join> joins = subjoin.getJoinList();
        for (Join j : joins) {
            // All the right items should be tables, too
            this.visit(j.getRightItem());
        }
    }

    @Override
    public void visit(SubSelect subSelect) {
        throw new UnsupportedOperationException("This operation is not supported!");
    }

    @Override
    public void visit(LateralSubSelect lateralSubSelect) {
        throw new UnsupportedOperationException("This operation is not supported!");
    }

    @Override
    public void visit(ValuesList valuesList) {
        throw new UnsupportedOperationException("This operation is not supported!");
    }

    @Override
    public void visit(TableFunction tableFunction) {
        throw new UnsupportedOperationException("This operation is not supported!");
    }

    @Override
    public void visit(ParenthesisFromItem item) {
        this.visit(item.getFromItem());
    }


    // ####################################### Rewriting ########################################


    /**
     * Copy the given table
     * @param table Table to be copied
     * @return Copy of the given table
     */
    private Table copyTable(Table table) {
        if (table == null)
            return null;
        Table copy = new Table(table.getName());
        Alias alias = table.getAlias();
        if (alias != null)
            copy.setAlias(new Alias(alias.getName(), alias.isUseAs()));
        return copy;
    }


    private void visit(FromItem from) {
        // Check which subtype matches
        if (from instanceof Table)
            this.visit((Table) from);
        else if (from instanceof SubJoin)
            this.visit((SubJoin) from);
        else if (from instanceof SubSelect)
            this.visit((SubSelect) from);
        else if (from instanceof LateralSubSelect)
            this.visit((LateralSubSelect) from);
        else if (from instanceof ValuesList)
            this.visit((ValuesList) from);
        else if (from instanceof TableFunction)
            this.visit((TableFunction) from);
        else if (from instanceof ParenthesisFromItem)
            this.visit((ParenthesisFromItem) from);
    }



    /**
     * Entry point. Takes a query body and rewrites the from clause of it according to the given fragment id of the
     * "ILL"-relation.
     * @param select Select statement
     * @param fragID ID of the fragment
     * @param allowOverwritingFragments Whether fragment expressions are overwritten
     * @return Pair consisting of rewritten table and a list of rewritten joins (note: the list may be empty)
     */
    public static Pair<Table, List<Join>> rewrite(Select select, int fragID, boolean allowOverwritingFragments) {
        return FromClauseRewriter.rewrite(select, fragID, allowOverwritingFragments, null);
    }


    /**
     * Entry point. Takes a query body and rewrites the from clause of it according to the given fragment id of the
     * "ILL"-relation and only the corresponding relation instance is considered.
     * @param select Select statement
     * @param fragID ID of the fragment
     * @param allowOverwritingFragments Wheter fragment expressions are overwritten
     * @param instance Relation instance
     * @return Pair consisting of rewritten table and a list of rewritten joins (note: the list may be empty)
     */
    public static Pair<Table, List<Join>> rewrite(Select select, int fragID, boolean allowOverwritingFragments,
                                                  String instance) {

        // Rewriting
        FromClauseRewriter rewriter = new FromClauseRewriter(select, fragID, allowOverwritingFragments, instance);
        PlainSelect body = (PlainSelect) select.getSelectBody();
        rewriter.visit(body.getFromItem());

        // Visit joins as well
        List<Join> joins = body.getJoins();
        if (joins != null) {
            for (Join j : joins) {
                rewriter.visit(j.getRightItem());
            }
        }

        // Only simple joins are allowed! Order doesn't matter
        List<Table> tables = rewriter.rewrittenTables;
        Table tab = tables.get(0);

        // if there are no more tables, this loop will be simply skipped and the list will be empty!
        joins = new ArrayList<>();
        for (int i = 1; i < tables.size(); i++) {
            Join j = new Join();
            j.setSimple(true);
            j.setRightItem(tables.get(i));
            joins.add(j);
        }

        return new ImmutablePair<>(tab, joins);
    }

}
