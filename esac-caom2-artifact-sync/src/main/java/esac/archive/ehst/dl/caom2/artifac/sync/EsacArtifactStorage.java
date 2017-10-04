package esac.archive.ehst.dl.caom2.artifac.sync;

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
import java.security.AccessControlException;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.sql.SQLException;
import java.util.logging.Logger;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import ca.nrc.cadc.caom2.artifactsync.ArtifactStore;
import ca.nrc.cadc.net.TransientException;
import ca.nrc.cadc.util.StringUtil;
import esac.archive.ehst.dl.caom2.artifac.sync.checksums.EsacChecksumManagement;

/**
 * ESAC Implementation of the ArtifactStore interface.
 *
 * This class interacts with the ESAC archive data web service to perform the
 * artifact operations defined in ArtifactStore.
 *
 * @author jduran
 */
public class EsacArtifactStorage implements ArtifactStore
{

    private static final Logger log = Logger.getLogger(EsacArtifactStorage.class.getName());

    String dataURL;

    public EsacArtifactStorage()
            throws IllegalArgumentException, SecurityException, IllegalAccessException, InvocationTargetException, NoSuchMethodException, SQLException, PropertyVetoException
    {
    }

    @Override
    public boolean contains(URI artifactURI, URI checksum) throws TransientException
    {
        init();
        boolean result = EsacChecksumManagement.getInstance().select(artifactURI, checksum);
        return result;
    }

    @Override
    public void store(URI artifactURI, URI checksum, Long arg2, InputStream input)
            throws TransientException, UnsupportedOperationException, IllegalArgumentException, AccessControlException, IllegalStateException
    {
        init();
        if (!contains(artifactURI, checksum))
        {
            if (!contains(artifactURI))
            {
                EsacChecksumManagement.getInstance().insert(artifactURI, checksum);
            }
            else
            {
                EsacChecksumManagement.getInstance().update(artifactURI, checksum);
            }
        }
    }

    private void init()
    {

    }

    private boolean contains(URI artifactURI) throws TransientException
    {
        boolean result = EsacChecksumManagement.getInstance().select(artifactURI);
        return result;
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