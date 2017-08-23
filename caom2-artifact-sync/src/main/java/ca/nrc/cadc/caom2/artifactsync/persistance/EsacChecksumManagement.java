package ca.nrc.cadc.caom2.artifactsync.persistance;

import java.lang.reflect.InvocationTargetException;
import java.net.URI;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.List;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;

import ca.nrc.cadc.caom2.artifactsync.esacdb.ConfigProperties;
import ca.nrc.cadc.caom2.artifactsync.esacdb.JdbcSingleton;

/**
 *
 * @author jduran
 *
 */
public class EsacChecksumManagement
{
    private static EsacChecksumManagement instance = null;

    private static final Logger log = Logger.getLogger(EsacChecksumManagement.class);

    private PreparedStatement preparedStatement = null;

    protected static final String SCHEMA = "caom2.artifactsync.checksumTable.schema";
    protected static final String TABLENAME = "caom2.artifactsync.checksumTable.table";
    protected static final String COLUMN_ARTIFACT = "caom2.artifactsync.checksumTable.columnArtifact";
    protected static final String COLUMN_CHECKSUM = "caom2.artifactsync.checksumTable.columnChecksum";
    private static String checksumSchema = null;
    private static String checksumTable = null;
    private static String checksumArtifactColumnName = null;
    private static String checksumChecksumColumnName = null;

    public static EsacChecksumManagement getInstance()
    {
        if (instance == null)
        {
            instance = new EsacChecksumManagement();
        }
        return instance;
    }
    private EsacChecksumManagement()
    {
        EsacChecksumManagement.checksumSchema = ConfigProperties.getInstance().getProperty(SCHEMA);
        EsacChecksumManagement.checksumTable = ConfigProperties.getInstance().getProperty(TABLENAME);
        EsacChecksumManagement.checksumArtifactColumnName = ConfigProperties.getInstance().getProperty(COLUMN_ARTIFACT);
        EsacChecksumManagement.checksumChecksumColumnName = ConfigProperties.getInstance().getProperty(COLUMN_CHECKSUM);

        try
        {
            preparedStatement = createPreparedStatement(JdbcSingleton.getInstance().getConnection(), checksumSchema, checksumTable);
        }
        catch (Exception e)
        {
            throw new RuntimeException("Unexpected exception: " + e.getMessage());
        }
    }

    private static PreparedStatement createPreparedStatement(Connection con, String schema, String tableName)
            throws IllegalArgumentException, SecurityException, IllegalAccessException, InvocationTargetException, NoSuchMethodException, SQLException
    {
        PreparedStatement insertValues = null;

        String table = schema + "." + tableName;

        String columns = checksumArtifactColumnName + "," + checksumChecksumColumnName;
        String values = "?,?";

        String insertStatement = "insert into " + table + "(" + columns + ")" + " values (" + values + ");";

        log.log(Level.INFO, "Insertion prepared statement " + insertStatement);

        insertValues = con.prepareStatement(insertStatement);

        return insertValues;
    }

    private static int databaseWriter(PreparedStatement insertValues, List<EsacChecksumPersistance> sources) throws Exception
    {
        int idx = 1;
        for (EsacChecksumPersistance source : sources)
        {
            insertValues.setString(idx++, source.getArtifactURI().toString());
            insertValues.setString(idx++, source.getChecksum().toString());
        }
        insertValues.addBatch();
        insertValues.executeBatch();
        insertValues.getConnection().commit();

        return sources.size();
    }

    public boolean select(URI artifactURI)
    {
        boolean result = false;
        Statement stmt = null;
        String query = "select * from " + checksumSchema + "." + checksumTable + " where " + checksumArtifactColumnName + " = '" + artifactURI.toString() + "';";
        ResultSet rs = null;

        try
        {
            Connection con = JdbcSingleton.getInstance().getConnection();
            stmt = con.createStatement();
            rs = stmt.executeQuery(query);
            if (rs.next())
            {
                result = true;
            }
            else
            {
                result = false;
            }

        }
        catch (Exception ex)
        {
            throw new RuntimeException("Unexpected exception: " + ex.getMessage());
        }
        finally
        {
            if (rs != null)
            {
                try
                {
                    rs.close();
                }
                catch (SQLException e)
                {
                }
            }
            if (stmt != null)
            {
                try
                {
                    stmt.close();
                }
                catch (SQLException e)
                {
                }
            }
        }
        return result;

    }

    public boolean select(URI artifactURI, URI checksum)
    {
        boolean result = false;
        Statement stmt = null;
        String query = "select * from " + checksumSchema + "." + checksumTable + " where " + checksumArtifactColumnName + " = '" + artifactURI.toString() + "' and "
                + checksumChecksumColumnName + " = '" + checksum.toString() + "'";
        ResultSet rs = null;

        try
        {
            Connection con = JdbcSingleton.getInstance().getConnection();
            stmt = con.createStatement();
            rs = stmt.executeQuery(query);
            if (rs.next())
            {
                result = true;
            }
            else
            {
                result = false;
            }

        }
        catch (Exception ex)
        {
            throw new RuntimeException("Unexpected exception: " + ex.getMessage());
        }
        finally
        {
            if (rs != null)
            {
                try
                {
                    rs.close();
                }
                catch (SQLException e)
                {
                }
            }
            if (stmt != null)
            {
                try
                {
                    stmt.close();
                }
                catch (SQLException e)
                {
                }
            }
        }
        return result;
    }

    public void insert(URI artifactURI, URI checksum)
    {
        try
        {
            List<EsacChecksumPersistance> list = new ArrayList<EsacChecksumPersistance>();
            list.add(new EsacChecksumPersistance(artifactURI, checksum));
            databaseWriter(preparedStatement, list);
        }
        catch (Exception ex)
        {
            throw new RuntimeException("Unexpected exception: " + ex.getMessage());
        }

    }

    public void update(URI artifactURI, URI checksum)
    {
        Statement stmt = null;
        String update = "update " + checksumSchema + "." + checksumTable + " set " + checksumChecksumColumnName + " = '" + checksum.toString() + "' where "
                + checksumArtifactColumnName + " = '" + artifactURI.toString() + "';";
        ResultSet rs = null;

        try
        {
            Connection con = JdbcSingleton.getInstance().getConnection();
            stmt = con.createStatement();
            int res = stmt.executeUpdate(update);
            if (res != 1)
            {
                throw new RuntimeException("Unexpected exception updating artifact: " + artifactURI.toString());
            }
        }
        catch (Exception ex)
        {
            throw new RuntimeException("Unexpected exception: " + ex.getMessage());
        }
        finally
        {
            if (rs != null)
            {
                try
                {
                    rs.close();
                }
                catch (SQLException e)
                {
                }
            }
            if (stmt != null)
            {
                try
                {
                    stmt.close();
                }
                catch (SQLException e)
                {
                }
            }
        }

    }

}
