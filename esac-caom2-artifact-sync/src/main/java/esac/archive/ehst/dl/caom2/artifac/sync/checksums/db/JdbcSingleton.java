package esac.archive.ehst.dl.caom2.artifac.sync.checksums.db;

import java.beans.PropertyVetoException;

/**
 *
 * @author jduran
 *
 */
public class JdbcSingleton extends Jdbc
{

    protected JdbcSingleton() throws PropertyVetoException
    {
        super();
    }

    private static JdbcSingleton instance = null;

    private synchronized static void createInstance() throws PropertyVetoException
    {
        if (instance == null)
        {
            instance = new JdbcSingleton();
        }
    }

    public static JdbcSingleton getInstance() throws PropertyVetoException
    {
        createInstance();
        return instance;
    }

}