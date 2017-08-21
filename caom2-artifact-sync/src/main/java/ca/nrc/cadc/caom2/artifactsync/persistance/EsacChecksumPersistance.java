package ca.nrc.cadc.caom2.artifactsync.persistance;

import java.net.URI;

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
