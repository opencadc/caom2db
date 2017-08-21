package ca.nrc.cadc.caom2.artifactsync.esacdb;

import java.beans.PropertyVetoException;

public class JdbcSingleton extends Jdbc
{

    protected JdbcSingleton() throws PropertyVetoException
    {
        super();
    }

    private static JdbcSingleton INSTANCE = null;

    private synchronized static void createInstance() throws PropertyVetoException
    {
        if (INSTANCE == null)
        {
            INSTANCE = new JdbcSingleton();
        }
    }

    public static JdbcSingleton getInstance() throws PropertyVetoException
    {
        createInstance();
        return INSTANCE;
    }

}
