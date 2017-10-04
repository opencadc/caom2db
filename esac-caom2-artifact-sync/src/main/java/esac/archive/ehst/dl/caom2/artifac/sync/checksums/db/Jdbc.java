package esac.archive.ehst.dl.caom2.artifac.sync.checksums.db;

import java.beans.PropertyVetoException;
import java.sql.Connection;
import java.sql.SQLException;

import com.mchange.v2.c3p0.ComboPooledDataSource;

/**
 *
 * @author jduran
 *
 */
public class Jdbc
{
    protected static final String DB_URL_PROP = "esac.tools.db.url";
    protected static final String DB_DRIVER_PROP = "esac.tools.db.driver";
    protected final static String DB_USER_PROP = "esac.tools.db.username";
    protected final static String DB_PWD_PROP = "esac.tools.db.password";

    protected ComboPooledDataSource cpds;

    public String getOwner()
    {
        return ConfigProperties.getInstance().getProperty(DB_USER_PROP);
    }

    public synchronized Connection getConnection() throws SQLException
    {
        return this.cpds.getConnection();
    }

    protected Jdbc() throws PropertyVetoException
    {

        String driver = ConfigProperties.getInstance().getProperty(DB_DRIVER_PROP);
        String owner = ConfigProperties.getInstance().getProperty(DB_USER_PROP);
        String password = ConfigProperties.getInstance().getProperty(DB_PWD_PROP);
        String url = ConfigProperties.getInstance().getProperty(DB_URL_PROP);

        cpds = new ComboPooledDataSource();
        cpds.setDriverClass(driver); // loads the jdbc driver
        cpds.setJdbcUrl(url);
        cpds.setUser(owner);
        cpds.setPassword(password);

        // the settings below are optional -- c3p0 can work with defaults
        cpds.setMinPoolSize(5);
        cpds.setAcquireIncrement(5);
        cpds.setMaxPoolSize(50);

    }

}