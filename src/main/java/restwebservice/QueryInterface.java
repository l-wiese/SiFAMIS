package restwebservice;

import clusteringbasedfragmentation.ClusteringAffinityFunction;
import clusteringbasedfragmentation.SimilarityException;
import net.sf.jsqlparser.JSQLParserException;
import net.sf.jsqlparser.expression.Expression;
import net.sf.jsqlparser.expression.operators.relational.EqualsTo;
import net.sf.jsqlparser.parser.CCJSqlParserUtil;
import net.sf.jsqlparser.statement.select.PlainSelect;
import net.sf.jsqlparser.statement.select.Select;
import org.apache.ignite.Ignite;
import org.apache.ignite.Ignition;
import org.apache.ignite.cache.query.FieldsQueryCursor;
import org.apache.ignite.cache.query.SqlFieldsQuery;
import org.apache.ignite.configuration.IgniteConfiguration;
import referenceimplementation.FlexibleQueryAnswering;
import rewriting.QueryRewriter;
import rewriting.RelaxationAttributeSelectionFinder;
import utils.IgniteUtils;
import utils.SQLQueryUtils;

import javax.servlet.ServletContext;
import javax.ws.rs.*;
import javax.ws.rs.core.*;
import java.io.IOException;
import java.sql.*;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;

// The Java class will be hosted at the URI path "/"
@Path("/query")
public class QueryInterface {

    @Context
    UriInfo uriInfo;

    @Context
    private ServletContext context;

    /**
     * Query timeout in seconds.
     */
    private static final int QUERY_TIMEOUT = 600;


    @GET
    @Produces(MediaType.TEXT_PLAIN)
    public String test() {
        return "Okay!";
    }

    @POST
    @Consumes(MediaType.APPLICATION_FORM_URLENCODED)
    public Response testQuery(MultivaluedMap<String, String> params) throws IOException, SQLException,
            ClassNotFoundException, JSQLParserException, SimilarityException {

        // DEBUG
        StringBuilder stringBuilder = new StringBuilder();
        for (String key : params.keySet()) {
            stringBuilder.append(key + " : " + params.getFirst(key) + "<br/>");
        }
        stringBuilder.append("<br/>");

        // Get form input
        String query = params.getFirst("query");
        String mode = params.getFirst("mode");
        Integer terms = new Integer(params.getFirst("terms"));
        Double alpha = new Double(params.getFirst("alpha"));
        boolean fqaEnabled = params.containsKey("fqa");

        stringBuilder.append(query + "," + mode + "," + terms + "," + alpha);

        // Execute query according to the parameters
        String table = null;
        if (mode.equals("ref")) {

            // Reference implementation
            ResultSet res = processRefImplQuery(query, fqaEnabled);
            table = resultSetToHTML(res);


        } else {

            // Clustering Affinity Function
            ClusteringAffinityFunction affinityFunction;
            affinityFunction = new ClusteringAffinityFunction(alpha, context.getRealPath("csv/terms" + terms +
                    ".txt"), context.getRealPath("csv/result" + terms + ".csv"));

            // Process according to implementation mode
            if (mode.equals("mat")) {
                // Materialized Fragment Approach
                ResultSet res = processMatFragQuery(query, affinityFunction, fqaEnabled);
                table = resultSetToHTML(res);

            } else if (mode.equals("par")) {
                // Partition Number Approach
                Object obj = processParNumQuery(query, affinityFunction, fqaEnabled);
                if (obj instanceof ResultSet) {
                    table = resultSetToHTML((ResultSet) obj);
                } else if (obj instanceof FieldsQueryCursor) {
                    table = cursorToHTML((FieldsQueryCursor<List<?>>) obj);
                }
            }
        }

        if (table == null)
            return Response.serverError()
                    .entity("Some fatal error occured while processing the query and the input!")
                    .build();

        return Response.ok().entity(table).build();
    }


    /**
     * Outputs the given {@link ResultSet} in form of an HTML table.
     *
     * @param res ResultSet of query execution
     * @return String
     * @throws SQLException
     */
    private String resultSetToHTML(ResultSet res) throws SQLException {

        // Build html table with query metadata
        StringBuilder html = new StringBuilder();
        html.append("<P ALIGN='center'><TABLE BORDER=1>");
        ResultSetMetaData rsmd = res.getMetaData();
        int columnCount = rsmd.getColumnCount();

        // Table header with column labels
        html.append("<TR>");
        for (int i = 0; i < columnCount; i++) {
            html.append("<TH>" + rsmd.getColumnLabel(i + 1) + "</TH>");
        }
        html.append("</TR>");

        // Data = Results of the query
        while (res.next()) {
            html.append("<TR>");
            for (int i = 0; i < columnCount; i++) {
                html.append("<TD>" + res.getString(i + 1) + "</TD>");
            }
            html.append("</TR>");
        }
        html.append("</TABLE></P>");

        // return built html table
        return html.toString();
    }

    /**
     * Outputs the given {@link FieldsQueryCursor} in form of an HTML table.
     * @param cursor Cursor of a query execution
     * @return String
     */
    private String cursorToHTML(FieldsQueryCursor<List<?>> cursor) {


        // Start with column names as table header
        StringBuilder html = new StringBuilder();
        html.append("<P ALIGN='center'><TABLE BORDER=1>");
        int columnCount = cursor.getColumnsCount();
        html.append("<TR>");
        for (int i = 0; i < columnCount; i++) {
            html.append("<TH>" + cursor.getFieldName(i) + "</TH>");
        }
        html.append("</TR>");

        // Data = Results of the query
        List<List<?>> rows = cursor.getAll();
        for (List<?> row : rows) {
            html.append("<TR>");
            for (int i = 0; i < columnCount; i++) {
                html.append("<TD>" + row.get(i) + "</TD>");
            }
            html.append("</TR>");
        }
        html.append("</TABLE></P>");

        // return built html table
        return html.toString();
    }


    /**
     * Process the query under the reference implementation.
     *
     * @param query      Sql Query
     * @param fqaEnabled If true, then the query is answered flexibly wrt. the clustering of all 100
     *                   terms with alpha=0.12
     * @return Result of query execution
     * @throws SQLException
     * @throws ClassNotFoundException
     * @throws SimilarityException If an exception occurs while calculating similarity
     */
    private ResultSet processRefImplQuery(String query, boolean fqaEnabled)
            throws SQLException, ClassNotFoundException, JSQLParserException, SimilarityException {

        // Flexible answering?
        if (fqaEnabled) {
            ClusteringAffinityFunction affinityFunction = new ClusteringAffinityFunction(0.12,
                    context.getRealPath("csv/terms100.txt"), context.getRealPath("csv/result100.csv"));
            String generalized = FlexibleQueryAnswering.generalize(query, affinityFunction);
            query = generalized;
        }

        // Establish JDBC connection
        try (Connection conn = SQLQueryUtils.getConnection("127.0.0.1")) {
            Statement stmt = conn.createStatement();
            stmt.setQueryTimeout(QUERY_TIMEOUT);
            return stmt.executeQuery(query);
        }
    }


    /**
     * Process the query under the materialized fragment implementation.
     *
     * @param query            Sql Query
     * @param affinityFunction Clustering Affinity Function
     * @param fqaEnabled       If true, then the query is answered flexibly wrt. the provided affinity function (clustering)
     * @return Query result
     * @throws JSQLParserException
     * @throws ClassNotFoundException
     * @throws SQLException
     */
    private ResultSet processMatFragQuery(String query, ClusteringAffinityFunction affinityFunction, boolean fqaEnabled)
            throws JSQLParserException, ClassNotFoundException, SQLException, SimilarityException {

        // rewrite query
        QueryRewriter rewriter = new QueryRewriter(affinityFunction);
        String q = rewriter.rewrite(query);

        if (fqaEnabled) {
            q = materializedfragments.FlexibleQueryAnswering.generalize(q, affinityFunction);
        }

        // Establish JDBC connection
        try (Connection conn = SQLQueryUtils.getConnection("127.0.0.1")) {
            Statement stmt = conn.createStatement();
            stmt.setQueryTimeout(QUERY_TIMEOUT);
            return stmt.executeQuery(q);
        }
    }


    /**
     * Process the query under the partition number implementation
     *
     * @param query            Sql Query
     * @param affinityFunction Clustering Affinity Function
     * @param fqaEnabled       If true, then the query is answered flexibly wrt. the provided affinity function (clustering)
     * @return Query result (that can either be a {@link ResultSet} or a {@link FieldsQueryCursor})
     * @throws JSQLParserException
     * @throws SQLException
     * @throws ClassNotFoundException
     */
    private Object processParNumQuery(String query, ClusteringAffinityFunction affinityFunction, boolean fqaEnabled)
            throws JSQLParserException, SQLException, ClassNotFoundException    {

        // Query to be executed


        // Parse query to obtain selection conditions on relax. attribute
        RelaxationAttributeSelectionFinder finder = new RelaxationAttributeSelectionFinder();
        net.sf.jsqlparser.statement.Statement stmt = CCJSqlParserUtil.parse(query);
        Select select = (Select) stmt;
        PlainSelect body = (PlainSelect) select.getSelectBody();
        Expression where = body.getWhere();

        // Found some selections?
        int[] partitions = null;
        HashSet<Integer> partitionSet;
        if (finder.findRelaxationAttributeSelections(where)) {
            // Get all disease terms and corresponding partitions to the found selections
            ArrayList<EqualsTo> selections = finder.getRelaxationAttributeSelections();
            partitionSet = SQLQueryUtils.getPartitionsForSelections(selections, affinityFunction);
            partitions = new int[partitionSet.size()];
            int i = 0;
            for (int p : partitionSet) {
                partitions[i] = p;
                i++;
            }

            // FieldsQuery with partitions
            SqlFieldsQuery fieldsQuery = new SqlFieldsQuery(query);
            fieldsQuery.setPartitions(partitions).setCollocated(true);
            IgniteConfiguration config =
                    IgniteUtils.createIgniteConfig(Arrays.asList("127.0.0.1:47500..47509"), true);
            try (Ignite client = Ignition.start(config)) {

                // Flexible Answering --> generalize query
                if (fqaEnabled) {
                    String generalized = partitionnumbers.FlexibleQueryAnswering.generalize(query, affinityFunction);
                    fieldsQuery.setSql(generalized);
                }
                return client.cache("SQL_PUBLIC_0").query(fieldsQuery);
            }

        } else {

            // Simply execute query via JDBC
            if (fqaEnabled) {
                query = partitionnumbers.FlexibleQueryAnswering.generalize(query, affinityFunction);
            }

            try (Connection conn = SQLQueryUtils.getConnection("127.0.0.1:10800")) {
                Statement statement = conn.createStatement();
                statement.setQueryTimeout(QUERY_TIMEOUT);
                return statement.executeQuery(query);
            }
        }
    }
}
