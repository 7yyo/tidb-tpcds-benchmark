import java.sql.SQLOutput;
import java.util.Map;
import java.util.Properties;

public interface Run {

    void run(Source source, Map<String, String> properties);

}
