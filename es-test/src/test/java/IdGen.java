import java.security.SecureRandom;
import java.util.UUID;

/**
 * Created by lisiyu on 2016/11/10.
 */
public abstract class IdGen {

    @SuppressWarnings("unused")
    private static SecureRandom random = new SecureRandom();

    public static String uuid(){
        return UUID.randomUUID().toString().replace("-", "");
    }


}
