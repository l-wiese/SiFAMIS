package restwebservice;

import javax.ws.rs.core.Application;
import java.util.HashSet;
import java.util.Set;

@javax.ws.rs.ApplicationPath("/SiFAMIS")
public class ApplicationPath extends Application {

    @Override
    public Set<Class<?>> getClasses() {
        final Set<Class<?>> classes = new HashSet<>();
        // register root resource
        classes.add(QueryInterface.class);
        classes.add(LandingPage.class);
        return classes;
    }

}
