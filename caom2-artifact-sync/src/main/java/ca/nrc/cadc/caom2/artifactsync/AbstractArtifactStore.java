package ca.nrc.cadc.caom2.artifactsync;

import java.io.InputStream;
import java.net.URI;
import java.net.URL;
import java.security.AccessControlException;
import java.util.Set;

import ca.nrc.cadc.caom2.artifact.ArtifactMetadata;
import ca.nrc.cadc.caom2.artifact.ArtifactStore;
import ca.nrc.cadc.caom2.artifactsync.StorageResolverFactory;
import ca.nrc.cadc.net.StorageResolver;
import ca.nrc.cadc.net.TransientException;
import ca.nrc.cadc.util.FileMetadata;

public abstract class AbstractArtifactStore implements ArtifactStore {
    protected final StorageResolverFactory resolverFactory = new StorageResolverFactory();
    
    @Override
    public URL toURL(URI uri) throws IllegalArgumentException {
       StorageResolver resolver = resolverFactory.getStorageResolver(uri); 
       
       if(resolver!=null) {
           return resolver.toURL(uri);
       }else {
           throw new IllegalArgumentException("No suitable StorageResolver found for URI: "+uri.toString());
       }
    }

    @Override
    public abstract boolean contains(URI artifactURI, URI checksum) 
            throws TransientException, UnsupportedOperationException, IllegalArgumentException, AccessControlException, IllegalStateException;

    @Override
    public abstract void store(URI artifactURI, InputStream data, FileMetadata metadata)
            throws TransientException, UnsupportedOperationException, IllegalArgumentException, AccessControlException, IllegalStateException;

    @Override
    public abstract Set<ArtifactMetadata> list(String archive) throws TransientException, UnsupportedOperationException, AccessControlException;

    @Override
    public abstract String toStorageID(String artifactURI) throws IllegalArgumentException;

    @Override
    public abstract void processResults(long total, long successes, long totalElapsedTime, long totalBytes, int threads);
}
