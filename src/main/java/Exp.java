import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class Exp {

    private static final Logger LOG = LoggerFactory.getLogger(Exp.class);


    public static void main(String[] args) {
        LOG.info("HELLO");

        LOG.trace("integer={}, value={}", 5 + 3, "hi");

        LOG.error("BYE");

    }
}
