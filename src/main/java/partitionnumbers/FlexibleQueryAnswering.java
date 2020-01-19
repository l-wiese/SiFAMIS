package partitionnumbers;

import clusteringbasedfragmentation.ClusteringAffinityFunction;
import net.sf.jsqlparser.JSQLParserException;
import net.sf.jsqlparser.expression.Expression;
import net.sf.jsqlparser.parser.CCJSqlParserUtil;
import net.sf.jsqlparser.statement.select.PlainSelect;
import net.sf.jsqlparser.statement.select.Select;
import rewriting.RelaxationAttributeSelectionFinder;

/**
 * This class implements the functionality to generalize a given SQL query wrt.
 * to a clustering-based fragmentation.
 */
public class FlexibleQueryAnswering {

    /**
     * Generalize the relaxation attribute selections (disease) in the given SQL query where clause and return the
     * generalized query.
     *
     * @param sql SQL query
     * @return generalized query
     */
    public static String generalize(String sql, ClusteringAffinityFunction affinityFunction) throws JSQLParserException {

        // Parse the query
        Select select = (Select) CCJSqlParserUtil.parse(sql);
        PlainSelect body = (PlainSelect) select.getSelectBody();
        Expression where = body.getWhere();


        // Check for relaxation attribute selections
        RelaxationAttributeSelectionFinder finder = new RelaxationAttributeSelectionFinder();
        if (!finder.findRelaxationAttributeSelections(where)) {
            System.out.println("Could not find any relaxation attribute selection condition in the given query body: " +
                    body);
            return sql;
        }

        return RelaxationSelectionGeneralizer.generalizeSelections(sql);
    }

}
