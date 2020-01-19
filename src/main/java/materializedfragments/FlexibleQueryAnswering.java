package materializedfragments;

import clusteringbasedfragmentation.ClusteringAffinityFunction;
import partitionnumbers.RelaxationSelectionGeneralizer;
import net.sf.jsqlparser.JSQLParserException;
import net.sf.jsqlparser.expression.Expression;
import net.sf.jsqlparser.parser.CCJSqlParserUtil;
import net.sf.jsqlparser.statement.select.PlainSelect;
import net.sf.jsqlparser.statement.select.Select;
import rewriting.RelaxationAttributeSelectionFinder;

public class FlexibleQueryAnswering extends RelaxationSelectionGeneralizer {


    private ClusteringAffinityFunction affinity;

    public FlexibleQueryAnswering(ClusteringAffinityFunction affinity) {
        this.affinity = affinity;
    }

    /**
     * Generalizes the given query according to the clustering of the ClusteringAffinityFunction
     * @param sql Query (Note: Must already be rewritten to match the appropriate table fragments)
     * @param affinityFunction Provides clustering and similarity
     *
     */
    public static String generalize(String sql, ClusteringAffinityFunction affinityFunction) throws JSQLParserException {

        // Parse the query
        Select select = (Select) CCJSqlParserUtil.parse(sql);
        PlainSelect body = (PlainSelect) select.getSelectBody();
        Expression where = body.getWhere();

        RelaxationAttributeSelectionFinder finder = new RelaxationAttributeSelectionFinder();
        if (!finder.findRelaxationAttributeSelections(where)) {
            System.out.println("Could not find any relaxation attribute selection condition in the given query body: " +
                    body);  // DEBUG
            return sql;
        }

        FlexibleQueryAnswering generalizer = new FlexibleQueryAnswering(affinityFunction);
        generalizer.visit(where);
        body.setWhere(generalizer.buildWhereExpression());
        return body.toString();
    }

}
