package referenceimplementation;

import clusteringbasedfragmentation.Cluster;
import clusteringbasedfragmentation.ClusteringAffinityFunction;
import clusteringbasedfragmentation.SimilarityException;
import net.sf.jsqlparser.JSQLParserException;
import net.sf.jsqlparser.expression.Expression;
import net.sf.jsqlparser.expression.StringValue;
import net.sf.jsqlparser.expression.operators.relational.EqualsTo;
import net.sf.jsqlparser.expression.operators.relational.ExpressionList;
import net.sf.jsqlparser.expression.operators.relational.InExpression;
import net.sf.jsqlparser.parser.CCJSqlParserUtil;
import net.sf.jsqlparser.schema.Column;
import net.sf.jsqlparser.statement.Statement;
import net.sf.jsqlparser.statement.select.*;

import java.util.ArrayList;
import java.util.List;
import java.util.Set;

/**
 * <p>
 * This class implements the functionality for generalizing the relaxation attribute conditions in the
 * queries in the reference implementation approach by replacing the conditions on the disease term of the form
 * (ILL.disease = "diseaseTerm1") with an SQL "IN" expression that contains all possible disease terms that
 * belong to the same cluster as the term in the original condition,
 * e.g. ILL.disease IN [diseaseTerm1, diseaseTerm3, diseaseTerm28, ...]
 * </p>
 * For details see {@link RelaxationSelectionGeneralizer#makeInExpression(Column, String)}.
 */
public class RelaxationSelectionGeneralizer extends partitionnumbers.RelaxationSelectionGeneralizer {

    /**
     * Affinity function
     */
    private ClusteringAffinityFunction affinityFunction;

    /**
     * Flag indicating whether an SQL "IN" expression is contained or not.
     */
    private boolean containsInExpression;

    /**
     * List of SQL "IN" expressions to be used for the generalization.
     */
    private List<InExpression> inExpressions = new ArrayList<>();


    /**
     * Constructor for the query generalizer
     *
     * @param affinityFunction Affinity function containing the clustering and the similarity function
     */
    public RelaxationSelectionGeneralizer(ClusteringAffinityFunction affinityFunction) {
        super();
        this.affinityFunction = affinityFunction;
    }

    /**
     * Detect if this is a selection condition, and if so, then generalize it replacing the EqualsTo comparison with the
     * single constant symbol by an IN expression covering all constant symbols of the corresponding cluster.
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
            if (!column.getColumnName().matches("(?i)disease")) {
                this.expressions.add(equalsTo);
            } else {
                InExpression inExpression = null;
                try {
                    inExpression = makeInExpression(column, ((StringValue) right).getValue());
                } catch (SimilarityException e) {
                    e.printStackTrace();
                    System.exit(-1);
                }
                this.inExpressions.add(inExpression);
                this.expressions.add(inExpression);
            }
        } else if (right instanceof Column && left instanceof StringValue) {
            column = (Column) right;
            if (!column.getColumnName().matches("(?i)disease")) {
                this.expressions.add(equalsTo);
            } else {
                InExpression inExpression = null;
                try {
                    inExpression = makeInExpression(column, ((StringValue) right).getValue());
                } catch (SimilarityException e) {
                    e.printStackTrace();
                    System.exit(-1);
                }
                this.inExpressions.add(inExpression);
                this.expressions.add(inExpression);
            }
        } else {
            this.expressions.add(equalsTo);
        }
    }


    /**
     * Create an InExpression for the given column expression term that contains all disease terms that belong to the
     * same cluster as the given disease term.
     *
     * @param column  Column expression
     * @param disease Disease term
     * @return SQL "IN" Expression
     * @throws SimilarityException If an exception occurs while calculating similarity
     */
    private InExpression makeInExpression(Column column, String disease) throws SimilarityException {

        // Get terms of the corresponding cluster
        int clusterNumber = this.affinityFunction.identifyCluster(disease);
        Cluster<String> cluster = this.affinityFunction.getClusters().get(clusterNumber);
        Set<String> adom = cluster.getAdom();

        // Expression list
        List<Expression> list = new ArrayList<>();
        for (String s : adom) {
            list.add(new StringValue(s));
        }
        list.add(new StringValue(cluster.getHead()));   // dont forget the head!

        // Set flag & return InExpression
        this.containsInExpression = true;
        return new InExpression(column, new ExpressionList(list));
    }

    /**
     * Generalize the selection conditions and return the generalized query string.
     *
     * @param body             Query body
     * @param affinityFunction Affinity function to obtain clustering
     * @return Generalized Query
     */
    public static String generalizeSelections(PlainSelect body, ClusteringAffinityFunction affinityFunction) {


        RelaxationSelectionGeneralizer generalizer = new RelaxationSelectionGeneralizer(affinityFunction);

        // Generalize where
        Expression where = body.getWhere();
        generalizer.visit(where);
        Expression newWhere = generalizer.buildWhereExpression();

        // Check if an AllColumns (*) or AllTableColumns (ILL.*) is contained
        List<SelectItem> selectItems = body.getSelectItems();
        boolean containsAllColumns = false;
        for (SelectItem item : selectItems) {
            if (item instanceof AllColumns)
                containsAllColumns = true;
            if (item instanceof AllTableColumns) {
                AllTableColumns atc = (AllTableColumns) item;
                if (atc.getTable().getName().equals("ILL"))
                    containsAllColumns = true;
            }
        }

        // Check if no AllColumns expression ('*') is contained and if an SQL "IN" Expression is contained
        if (!containsAllColumns && generalizer.containsInExpression) {

            // Add relaxation attribute to select items (for each in expression) if not present yet
            for (InExpression inExpression : generalizer.inExpressions) {
                Column column = (Column) inExpression.getLeftExpression();
                SelectExpressionItem selectExpressionItem = new SelectExpressionItem(column);
                body.addSelectItems(selectExpressionItem);
            }
        }
        body.setWhere(newWhere);
        return body.toString();
    }


    /**
     * Test unit
     *
     * @param args Not used here
     */
    public static void main(String[] args) {

        String sampleQuery = "SELECT i.* FROM ILL i WHERE i.disease = 'Influenza'";

        try {
            ClusteringAffinityFunction affinityFunction = new ClusteringAffinityFunction();
            Statement stmt = CCJSqlParserUtil.parse(sampleQuery);
            Select selectStatement = (Select) stmt;
            PlainSelect select = (PlainSelect) selectStatement.getSelectBody();

            System.out.println(generalizeSelections(select, affinityFunction));

        } catch (SimilarityException e) {
            System.err.println("Could not get affinity function!");
            e.printStackTrace();
            System.exit(-1);
        } catch (JSQLParserException e) {
            System.err.println("Could not parse sample query  '" + sampleQuery + "'!");
            e.printStackTrace();
            System.exit(-1);
        }


    }

}
