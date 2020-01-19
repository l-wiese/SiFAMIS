package rewriting;

import clusteringbasedfragmentation.Clustering;
import clusteringbasedfragmentation.ClusteringAffinityFunction;
import clusteringbasedfragmentation.SimilarityException;
import net.sf.jsqlparser.JSQLParserException;
import net.sf.jsqlparser.expression.Expression;
import net.sf.jsqlparser.expression.StringValue;
import net.sf.jsqlparser.expression.operators.relational.EqualsTo;
import net.sf.jsqlparser.parser.CCJSqlParserUtil;
import net.sf.jsqlparser.schema.Column;
import net.sf.jsqlparser.schema.Table;
import net.sf.jsqlparser.statement.Statement;
import net.sf.jsqlparser.statement.select.*;
import net.sf.jsqlparser.util.TablesNamesFinder;
import org.apache.commons.lang3.tuple.Pair;

import java.io.File;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;

/**
 * This class is responsible for query rewriting for the materialized fragment approach where each fragment
 * of the relations is stored as a separate SQL table (relation) in the database with a name like "f_12" that indicates the
 * name of the relation(="f") and the id of the corresponding fragment (=12).
 * It takes a SQL-Query given as String and rewrites it according to the given clustering-based fragmentation
 * if it is possible.
 */
public class QueryRewriter {


    /**
     * Affinity function for the clustering-based fragmentation
     */
    private ClusteringAffinityFunction affinityFunction;

    /**
     * Clustering.
     */
    private Clustering clustering;


// ###################################### Constructors ########################################

    /**
     * Constructor for a query rewriter and a given affinity function (incl. clustering-based fragmentation).
     * @param affinityFunction Affinity function for the clustering-based fragmentation
     */
    public QueryRewriter(ClusteringAffinityFunction affinityFunction) {
        this.affinityFunction = affinityFunction;
        this.clustering = this.affinityFunction.getClustering();
    }


// ####################################### Rewriting ##########################################

    /**
     * Takes an SQL query against the medical information system and rewrites it according to the given
     * clustering-based, materialized fragmentation. If there is a selection condition on the relaxation attribute
     * (disease) of the clustering-based fragmentation, then the query can be easily rewritten by chosing the fragment
     * that corresponds to the cluster the disease terms belongs to. If there is no selection condition, the
     * localization program of the query has to be considered (union of all fragments --> distributed join).
     * NOTE: Complex queries containing subqueries or disjunctions in the WHERE clause are not supported.
     * @param sql SQL Query
     * @return Rewritten SQL Query
     * @throws UnsupportedExpressionException If any unsupported expression occurs while deparsing
     * @throws JSQLParserException JSQLParser exception upon parsing of the SQL String
     * @throws SimilarityException If an exception occurs while calculating similarity
     */
    public String rewrite(String sql) throws UnsupportedExpressionException, JSQLParserException, SimilarityException {

        String rewrittenSql;

        // Parse sql query
        Statement stmt = CCJSqlParserUtil.parse(sql);
        Select selectStatement = (Select) stmt;

        // Get the table names (reject if others than "ILL" and "INFO" are contained)
        TablesNamesFinder tablesNamesFinder = new TablesNamesFinder();
        List<String> tableList = tablesNamesFinder.getTableList(selectStatement);
        boolean illInQuery = false;
        for (String table : tableList) {        // No aliases
            if (!table.matches("(?i)info") && !table.matches("(?i)ill") && !table.matches("(?i)treat"))
                throw new IllegalArgumentException("Sql Query '" + sql + "' contains a wrong table name: " + table);
            if (table.matches("(?i)ill"))
                illInQuery = true;
        }

        if (!illInQuery)         // Rewriting is unnecessary!
            return sql;

        // Get WHERE clause if exists
        PlainSelect body = (PlainSelect) selectStatement.getSelectBody();
        Expression where = body.getWhere();
        if (where == null) {
            // No WHERE clause contained --> localization program
            SetOperationList localization = localizationProgram(selectStatement);
            selectStatement.setSelectBody(localization);
            return selectStatement.toString();

        } else {

            // Get the selections on the relaxation attribute
            RelaxationAttributeSelectionFinder whereParser = new RelaxationAttributeSelectionFinder();

            // Found some selection?
            if (whereParser.findRelaxationAttributeSelections(where)) {

                ArrayList<EqualsTo> relaxationAttributeSelections = whereParser.getRelaxationAttributeSelections();

                // Reject more than one selection condition on the same instance of relation ILL! Could be the case that
                // SELECT .. FROM ILL i1, ILL i2 WHERE i1.disease = ... AND i2.disease = ... which is fine!
                if (checkRelationInstancesOfRelaxSelections(relaxationAttributeSelections)) {
                    rewrittenSql = this.rewrite(selectStatement, relaxationAttributeSelections);
                } else {
                    throw new UnsupportedExpressionException("There are more than one selection conditions in the " +
                            "query '" + sql + "' on the relaxation attribute for the same relation instance!");
                }


            } else {
                // No selection conditions on relaxation attribute --> localization program
                SetOperationList localization = localizationProgram(selectStatement);
                selectStatement.setSelectBody(localization);
                return selectStatement.toString();
            }

        }

        return rewrittenSql;
    }


    /**
     * Create the localization program for the given {@link Select} statement
     * @param select Select statement
     * @return Localization program in form of a {@link SetOperationList}
     * @throws JSQLParserException Thrown if an error occurs related to the parsing of the Select statement
     */
    private SetOperationList localizationProgram(Select select) throws JSQLParserException {

        // For each instance of "ILL", "TREAT" and "INFO" relation, get localization program (consider all fragments)
        int frags = this.clustering.size();
        SetOperationList localization = new SetOperationList();

        // Brackets for all
        List<Boolean> brackets = new ArrayList<>(frags);
        for (int i = 0; i < frags; i++) {
            brackets.add(true);
        }

        // Unions for all (note: one less because n selects are connected via (n-1) unions!)
        List<SetOperation> ops = new ArrayList<>(frags);
        for (int i = 0; i < frags - 1; i++) {
            ops.add(new UnionOp());
        }

        // Select ...
        List<SelectBody> selects = new ArrayList<>(frags);
        Distinct distinct = ((PlainSelect)select.getSelectBody()).getDistinct();
        for (int i = 0; i < frags; i++) {
            PlainSelect rewrittenBody = this.rewriteBody(select, i);
            if (distinct != null)
                rewrittenBody.setDistinct(new Distinct());
            selects.add(rewrittenBody);
        }
        localization.setBracketsOpsAndSelects(brackets, selects, ops);
        return localization;
    }


    /**
     * Check for all the given EqualsTo selection conditions on the relaxation attribute whether there is maximum one
     * selection per table instance.
     * E.g. SELECT ... FROM ILL i1, ILL i2 WHERE ... AND i1.disease='...' AND i2.disease='...' is fine, but
     * SELECT ... FROM ILL i WHERE ... AND i.disease='...' AND i.disease='...' is rejected!
     * @param relaxSelections Relaxation attribute selection conditions
     * @return True, if the check was successful; otherwise false is returned
     * @throws UnsupportedExpressionException Thrown if an unsupported operation is contained in the query
     */
    private boolean checkRelationInstancesOfRelaxSelections(ArrayList<EqualsTo> relaxSelections)
        throws UnsupportedExpressionException{

        // If it is only one selection, it is fine
        if (relaxSelections.size() == 1)
            return true;

        // HashMap to store the number of selections on the relaxation attribute for a certain table instance
        HashMap<String, Integer> map = new HashMap<>();

        // For each selection, count the corresponding relation instances
        // NOTE: May produce undetermined behavoir if aliasing is mixed with unaliased column expressions ...
        for (EqualsTo selection : relaxSelections) {
            Column column = (Column) selection.getLeftExpression();

            // Name should be "Table.Column", "Tablealias.Column" or only "Column", else is rejected
            String name = column.getName(true);
            if (name.contains(".")) {
                String[] parts = name.split("\\.");
                if (parts.length != 2) {
                    throw new UnsupportedExpressionException("The EqualsTo expr. '" + selection + "' contains an " +
                            "unsupported expression as name for the column expression: " + name);
                }

                String tableOrAlias = parts[0];
                if (map.containsKey(tableOrAlias))
                    map.put(tableOrAlias, map.get(tableOrAlias) + 1);
                else
                    map.put(tableOrAlias, 1);

            }
        }

        // Now check if there are two or more selections on the relaxation attribute on the same instance
        for (String key : map.keySet())
            if (map.get(key) > 1)
                return false;

        return true;
    }


    /**
     * Rewrite the given Sql Query Body with the fragment id corresponding to the found relaxation selection
     * (disease='...MeSH term...').
     * @param select Select statement
     * @param relaxSelection Selection condition from the Sql query on the relaxation attribute (left expression is
     *                       a Column expression, right expression is a StringValue expression
     * @return Rewritten Sql Query
     * @throws UnsupportedExpressionException If any unsupported expression/type occurs while deparsing
     * @throws JSQLParserException If an exception occurs while parsing
     * @throws SimilarityException If an exception occurs while calculating similarity
     */
    private String rewrite(Select select, EqualsTo relaxSelection) throws UnsupportedExpressionException,
            JSQLParserException, SimilarityException {

        // Identify the fragment id
        String disease = ((StringValue) relaxSelection.getRightExpression()).getValue();
        int fragID = this.affinityFunction.identifyCluster(disease);

        // Rewrite Select, From and Where Clauses
        PlainSelect rewrittenBody = new PlainSelect();
        PlainSelect body = (PlainSelect) select.getSelectBody();
        rewrittenBody.setSelectItems(SelectClauseRewriter.rewrite(body, fragID,false));
        if (body.getDistinct() != null)
            rewrittenBody.setDistinct(new Distinct());

        Pair<Table, List<Join>> pair = FromClauseRewriter.rewrite(select, fragID, false);
        rewrittenBody.setFromItem(pair.getKey());
        if (! pair.getValue().isEmpty())
            rewrittenBody.setJoins(pair.getValue());

        Expression rewrittenWhere = WhereClauseRewriter.rewrite(body, fragID, false);
        if (rewrittenWhere != null)
            rewrittenBody.setWhere(rewrittenWhere);

        // Rewrite group by (if present)
        List<Expression> rewrittenGroupBy = GroupByRewriter.rewrite(body, fragID, false);
        if (rewrittenGroupBy != null)
            rewrittenBody.setGroupByColumnReferences(rewrittenGroupBy);

        // Finally, return the rewritten sql query string
        return rewrittenBody.toString();
    }


    /**
     * Rewrite the given Sql Query Body with the fragment id corresponding to the found relaxation selection
     * (disease='...MeSH term...') and the list of relaxation selections (that are all stated on different relation
     * instances, otherwise the behavior is undefined).
     * @param select Select statement
     * @param relaxSelections Selection conditions from the Sql query on the relaxation attribute (left expression is
     *                        a Column expression, right expression is a StringValue expression); all must be defined on
     *                        different relation instances
     * @return Rewritten Sql Query
     * @throws SimilarityException If an exception occurs while calculating similarity
     * @throws JSQLParserException If an exception occurs while parsing
     */
    private String rewrite(Select select, ArrayList<EqualsTo> relaxSelections) throws JSQLParserException, SimilarityException {

        if (relaxSelections.size() == 1) {
            return this.rewrite(select, relaxSelections.get(0));
        }

        // For each selection, rewrite the query accordingly
        PlainSelect body = (PlainSelect) select.getSelectBody();
        for (EqualsTo selection : relaxSelections) {

            // Get the fragment id
            String disease = ((StringValue) selection.getRightExpression()).getValue();
            int fragID = this.affinityFunction.identifyCluster(disease);

            // Get the relation instance, getName() should be "Table.disease" or "TableAlias.disease"
            Column column = (Column) selection.getLeftExpression();
            String instance = column.getName(true).split("\\.")[0];

            // Rewrite Select, From and Where clause
            body.setSelectItems(SelectClauseRewriter.rewrite(body, fragID, false));

            Pair<Table, List<Join>> pair =
                    FromClauseRewriter.rewrite(select, fragID, false, instance);
            body.setFromItem(pair.getKey());
            if (!pair.getValue().isEmpty())
                body.setJoins(pair.getValue());

            Expression rewrittenWhere = WhereClauseRewriter.rewrite(body, fragID, false);
            if (rewrittenWhere != null)
                body.setWhere(rewrittenWhere);

            // Rewrite group by (if present)
            List<Expression> rewrittenGroupBy = GroupByRewriter.rewrite(body, fragID, false);
            if (rewrittenGroupBy != null)
                body.setGroupByColumnReferences(rewrittenGroupBy);
        }

        return body.toString();
    }


    /**
     * Rewriting method for the localization program.
     * @param select Select statement
     * @param fragID Fragment ID
     * @return Rewritten Select statement
     */
    private PlainSelect rewriteBody(Select select, int fragID) throws JSQLParserException {

        // Rewrite Select, From and Where Clauses
        PlainSelect rewrittenBody = new PlainSelect();
        PlainSelect body = (PlainSelect) select.getSelectBody();
        rewrittenBody.setSelectItems(SelectClauseRewriter.rewrite(body, fragID, true));
        if (body.getDistinct() != null)
            rewrittenBody.setDistinct(new Distinct());

        Pair<Table, List<Join>> pair = FromClauseRewriter.rewrite(select, fragID, true);
        rewrittenBody.setFromItem(pair.getKey());
        if (! pair.getValue().isEmpty())
            rewrittenBody.setJoins(pair.getValue());

        Expression rewrittenWhere = WhereClauseRewriter.rewrite(body, fragID, true);
        if (rewrittenWhere != null)
            rewrittenBody.setWhere(rewrittenWhere);

        // Rewrite group by (if present)
        List<Expression> rewrittenGroupBy = GroupByRewriter.rewrite(body, fragID, true);
        if (rewrittenGroupBy != null)
            rewrittenBody.setGroupByColumnReferences(rewrittenGroupBy);

        // Return rewritten body
        return rewrittenBody;
    }




// #################################### Testing & Debug #######################################

    /**
     * Test unit
     * @param args Not used
     */
    public static void main(String[] args) throws SimilarityException {

        // Some sample affinity function with a clustering-based fragmentation
        String separ = File.separator;
        String termsFile = "csv" + separ + "terms100.txt";
        String simFile = "csv" + separ + "result100.csv";
        ClusteringAffinityFunction affinityFunction = new ClusteringAffinityFunction(0.12, termsFile, simFile);
        //affinityFunction.getClustering().printClusteringStatistics();

        // Rewriter
        QueryRewriter rewriter = new QueryRewriter(affinityFunction);
        List<String> samples = Arrays.asList(
                "SELECT i.Disease, p.* FROM ILL i, INFO p WHERE i.patientid = p.id",
                "SELECT p.Name, count(i.disease) FROM ILL i, INFO p WHERE i.patientid = p.id " +
                        "GROUP BY p.Name ORDER BY count(i.disease)",
                "SELECT avg(p.age) FROM INFO p",
                "SELECT p.Name, p.age, p.address FROM ILL i, INFO p WHERE i.patientid = p.id AND i.disease='Cough'",
                "SELECT p.Name, p.age, p.address, ILL.disease FROM ILL, INFO p WHERE ILL.patientid = p.id AND ILL.disease='Liver Failure'",
                "SELECT p.name, p.age, p.address FROM ILL i1, ILL i2, INFO p WHERE i1.patientid = p.id " +
                        "AND i2.patientid = p.id AND i1.disease='Trichuriasis' AND i2.disease='Blackwater Fever'");

        try {
            for (String sql : samples) {
                System.out.println("Query: " + sql);
                String rewritten = rewriter.rewrite(sql);
                System.out.println("Rewritten to --> " + rewritten);
            }
        } catch (UnsupportedExpressionException e) {
            System.err.println("ERROR! An unsupported expression was found!");
            e.printStackTrace();
        } catch (JSQLParserException e) {
            System.err.println("ERROR! The parser threw an exception!");
            e.printStackTrace();
        }


    }

}
