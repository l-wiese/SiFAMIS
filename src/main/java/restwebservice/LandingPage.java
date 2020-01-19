package restwebservice;

import javax.ws.rs.GET;
import javax.ws.rs.Path;
import javax.ws.rs.core.Context;
import javax.ws.rs.core.Response;
import javax.ws.rs.core.UriInfo;
import java.net.URI;
import java.net.URISyntaxException;

@Path("/")
public class LandingPage {

    @GET
    public Response redirect(@Context UriInfo uriInfo) throws URISyntaxException {

        URI location = uriInfo.getBaseUriBuilder().path("../QueryInterface.html").build();
        return Response.temporaryRedirect(location).build();
    }

}
