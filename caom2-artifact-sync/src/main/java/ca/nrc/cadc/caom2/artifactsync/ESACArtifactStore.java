/*
************************************************************************
*******************  CANADIAN ASTRONOMY DATA CENTRE  *******************
**************  CENTRE CANADIEN DE DONNÉES ASTRONOMIQUES  **************
*
*  (c) 2017.                            (c) 2017.
*  Government of Canada                 Gouvernement du Canada
*  National Research Council            Conseil national de recherches
*  Ottawa, Canada, K1A 0R6              Ottawa, Canada, K1A 0R6
*  All rights reserved                  Tous droits réservés
*
*  NRC disclaims any warranties,        Le CNRC dénie toute garantie
*  expressed, implied, or               énoncée, implicite ou légale,
*  statutory, of any kind with          de quelque nature que ce
*  respect to the software,             soit, concernant le logiciel,
*  including without limitation         y compris sans restriction
*  any warranty of merchantability      toute garantie de valeur
*  or fitness for a particular          marchande ou de pertinence
*  purpose. NRC shall not be            pour un usage particulier.
*  liable in any event for any          Le CNRC ne pourra en aucun cas
*  damages, whether direct or           être tenu responsable de tout
*  indirect, special or general,        dommage, direct ou indirect,
*  consequential or incidental,         particulier ou général,
*  arising from the use of the          accessoire ou fortuit, résultant
*  software.  Neither the name          de l'utilisation du logiciel. Ni
*  of the National Research             le nom du Conseil National de
*  Council of Canada nor the            Recherches du Canada ni les noms
*  names of its contributors may        de ses  participants ne peuvent
*  be used to endorse or promote        être utilisés pour approuver ou
*  products derived from this           promouvoir les produits dérivés
*  software without specific prior      de ce logiciel sans autorisation
*  written permission.                  préalable et particulière
*                                       par écrit.
*
*  This file is part of the             Ce fichier fait partie du projet
*  OpenCADC project.                    OpenCADC.
*
*  OpenCADC is free software:           OpenCADC est un logiciel libre ;
*  you can redistribute it and/or       vous pouvez le redistribuer ou le
*  modify it under the terms of         modifier suivant les termes de
*  the GNU Affero General Public        la “GNU Affero General Public
*  License as published by the          License” telle que publiée
*  Free Software Foundation,            par la Free Software Foundation
*  either version 3 of the              : soit la version 3 de cette
*  License, or (at your option)         licence, soit (à votre gré)
*  any later version.                   toute version ultérieure.
*
*  OpenCADC is distributed in the       OpenCADC est distribué
*  hope that it will be useful,         dans l’espoir qu’il vous
*  but WITHOUT ANY WARRANTY;            sera utile, mais SANS AUCUNE
*  without even the implied             GARANTIE : sans même la garantie
*  warranty of MERCHANTABILITY          implicite de COMMERCIALISABILITÉ
*  or FITNESS FOR A PARTICULAR          ni d’ADÉQUATION À UN OBJECTIF
*  PURPOSE.  See the GNU Affero         PARTICULIER. Consultez la Licence
*  General Public License for           Générale Publique GNU Affero
*  more details.                        pour plus de détails.
*
*  You should have received             Vous devriez avoir reçu une
*  a copy of the GNU Affero             copie de la Licence Générale
*  General Public License along         Publique GNU Affero avec
*  with OpenCADC.  If not, see          OpenCADC ; si ce n’est
*  <http://www.gnu.org/licenses/>.      pas le cas, consultez :
*                                       <http://www.gnu.org/licenses/>.
*
*  $Revision: 5 $
*
************************************************************************
*/

package ca.nrc.cadc.caom2.artifactsync;

import java.beans.PropertyVetoException;
import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.lang.reflect.InvocationTargetException;
import java.math.BigInteger;
import java.net.MalformedURLException;
import java.net.URI;
import java.net.URL;
import java.nio.MappedByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.channels.FileChannel.MapMode;
import java.nio.file.Paths;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;

import ca.nrc.cadc.caom2.artifactsync.esacdb.ConfigProperties;
import ca.nrc.cadc.caom2.artifactsync.esacdb.JdbcSingleton;
import ca.nrc.cadc.caom2.artifactsync.persistance.EsacChecksumPersistance;
import ca.nrc.cadc.net.TransientException;
import ca.nrc.cadc.util.StringUtil;

/**
 * ESAC Implementation of the ArtifactStore interface.
 *
 * This class interacts with the ESAC archive data web service to perform the
 * artifact operations defined in ArtifactStore.
 *
 * @author jduran
 */
public class ESACArtifactStore implements ArtifactStore
{
    private static final Logger log = Logger.getLogger(ESACArtifactStore.class);

    private PreparedStatement preparedStatement = null;

    protected static final String SCHEMA = "caom2.artifactsync.checksumTable.schema";
    protected static final String TABLENAME = "caom2.artifactsync.checksumTable.table";
    protected static final String COLUMN_ARTIFACT = "caom2.artifactsync.checksumTable.columnArtifact";
    protected static final String COLUMN_CHECKSUM = "caom2.artifactsync.checksumTable.columnChecksum";
    private static String checksumSchema = null;
    private static String checksumTable = null;
    private static String checksumArtifactColumnName = null;
    private static String checksumChecksumColumnName = null;

    String dataURL;

    public ESACArtifactStore()
            throws IllegalArgumentException, SecurityException, IllegalAccessException, InvocationTargetException, NoSuchMethodException, SQLException, PropertyVetoException
    {
        ESACArtifactStore.checksumSchema = ConfigProperties.getInstance().getProperty(SCHEMA);
        ESACArtifactStore.checksumTable = ConfigProperties.getInstance().getProperty(TABLENAME);
        ESACArtifactStore.checksumArtifactColumnName = ConfigProperties.getInstance().getProperty(COLUMN_ARTIFACT);
        ESACArtifactStore.checksumChecksumColumnName = ConfigProperties.getInstance().getProperty(COLUMN_CHECKSUM);

        preparedStatement = createPreparedStatement(JdbcSingleton.getInstance().getConnection(), checksumSchema, checksumTable);
    }

    @Override
    public boolean contains(URI artifactURI, URI checksum) throws TransientException
    {
        init();
        boolean result = select(artifactURI, checksum);
        return result;
    }

    @Override
    public void store(URI artifactURI, URI checksum, InputStream data) throws TransientException
    {
        init();
        if (!contains(artifactURI, checksum))
        {
            if (!contains(artifactURI))
            {
                insert(artifactURI, checksum);
            }
            else
            {
                update(artifactURI, checksum);
            }
        }
    }

    private void init()
    {

    }

    private boolean contains(URI artifactURI) throws TransientException
    {
        boolean result = select(artifactURI);
        return result;
    }

    private boolean select(URI artifactURI)
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

    private boolean select(URI artifactURI, URI checksum)
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

    private void insert(URI artifactURI, URI checksum)
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

    private void update(URI artifactURI, URI checksum)
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

    private String calculateMD5Sum(String path) throws UnsupportedOperationException
    {
        String md5sum = null;
        MessageDigest md = null;
        try
        {
            md = MessageDigest.getInstance("MD5");
        }
        catch (NoSuchAlgorithmException e)
        {
            throw new RuntimeException("Unexpected exception: " + e.getMessage());
        }
        try (FileChannel fc = FileChannel.open(Paths.get(path)))
        {
            long meg = 1024 * 1024;
            long len = fc.size();
            long pos = 0;
            while (pos < len)
            {
                long size = Math.min(meg, len - pos);
                MappedByteBuffer mbb = fc.map(MapMode.READ_ONLY, pos, size);
                md.update(mbb);
                pos += size;
            }
            // byte[] hash = md.digest();
            md5sum = String.format("%032x", new BigInteger(1, md.digest()));
        }
        catch (IOException e)
        {
            throw new RuntimeException("Unexpected exception: " + e.getMessage());
        }
        return md5sum;
    }

    private String getMD5Sum(URI checksum) throws UnsupportedOperationException
    {
        if (checksum == null)
        {
            return null;
        }

        if (checksum.getScheme().equalsIgnoreCase("MD5"))
        {
            return checksum.getSchemeSpecificPart();
        }
        else
        {
            throw new UnsupportedOperationException("Checksum algorithm " + checksum.getScheme() + " not suported.");
        }
    }

    private URL createDataURL(ESACDataURI dataURI)
    {
        try
        {
            return new URL(dataURL + "/" + dataURI.getArchivePath());
        }
        catch (MalformedURLException e)
        {
            throw new IllegalStateException("BUG in forming data URL", e);
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

    class ESACDataURI
    {

        /**
         * Path to the archive
         */
        private String archivePath;

        /**
         * Filew name
         */
        private String fileName;

        /**
         * Constructor
         *
         * @param completePath
         *            Complete path to the file
         * @throws UnsupportedOperationException
         */
        ESACDataURI(String completePath) throws UnsupportedOperationException
        {
            validate(completePath);
        }

        /**
         * Validates the complete path used to create ESACDataURI
         *
         * @param completePath
         *            Complete path to the file
         */
        private void validate(String completePath)
        {
            if (completePath == null || !StringUtil.hasText(completePath))
                throw new IllegalArgumentException("Not valid path " + completePath);

            File file = new File(completePath);
            if (!file.exists() || file.isDirectory())
            {
                file = null;
                throw new IllegalArgumentException("Cannot find archive in path " + completePath);
            }

            file = null;

            int i = completePath.lastIndexOf('/');
            if (i >= 0)
            {
                this.setFileName(completePath.substring(i + 1));
                this.setArchivePath(completePath);
            }

            sanitize(this.getArchivePath());
            sanitize(this.getFileName());

            if (completePath != null)
                throw new UnsupportedOperationException("Artifact namespace not supported");

            // TODO: Remove this trim when AD supports longer archive names
            // if (archivePath.length() > 6)
            // archivePath = archivePath.substring(0, 6);
        }

        private void sanitize(String s)
        {
            Pattern regex = Pattern.compile("^[a-zA-Z 0-9\\_\\.\\-\\+\\@]*$");
            Matcher matcher = regex.matcher(s);
            if (!matcher.find())
                throw new IllegalArgumentException("Invalid dataset characters.");
        }

        public String getArchivePath()
        {
            return archivePath;
        }

        private void setArchivePath(String archivePath)
        {
            this.archivePath = archivePath;
        }

        public String getFileName()
        {
            return fileName;
        }

        private void setFileName(String fileName)
        {
            this.fileName = fileName;
        }
    }

}
