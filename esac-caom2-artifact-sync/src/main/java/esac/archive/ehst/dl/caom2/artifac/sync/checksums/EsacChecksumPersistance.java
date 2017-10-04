package esac.archive.ehst.dl.caom2.artifac.sync.checksums;

import java.net.URI;

/**
 *
 * @author jduran
 *
 */
public class EsacChecksumPersistance
{
    private URI artifactURI;
    private URI checksum;

    public EsacChecksumPersistance(URI artifact, URI checksum)
    {
        this.setArtifactURI(artifact);
        this.setChecksum(checksum);
    }

    public URI getArtifactURI()
    {
        return artifactURI;
    }

    public void setArtifactURI(URI artifactURI)
    {
        this.artifactURI = artifactURI;
    }

    public URI getChecksum()
    {
        return checksum;
    }

    public void setChecksum(URI checksum)
    {
        this.checksum = checksum;
    }
}