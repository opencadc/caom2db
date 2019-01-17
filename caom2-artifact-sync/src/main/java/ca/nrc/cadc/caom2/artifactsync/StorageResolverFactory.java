package ca.nrc.cadc.caom2.artifactsync;

import ca.nrc.cadc.caom2.artifact.resolvers.AdResolver;
import ca.nrc.cadc.caom2.artifact.resolvers.GeminiResolver;
import ca.nrc.cadc.caom2.artifact.resolvers.MastResolver;
import ca.nrc.cadc.caom2.artifact.resolvers.NoaoResolver;
import ca.nrc.cadc.caom2.artifact.resolvers.SdssResolver;
import ca.nrc.cadc.caom2.artifact.resolvers.SubaruResolver;
import ca.nrc.cadc.caom2.artifact.resolvers.VOSpaceResolver;
import ca.nrc.cadc.caom2.artifact.resolvers.XmmResolver;
import ca.nrc.cadc.net.StorageResolver;

import java.net.URI;
import java.util.HashMap;
import java.util.Map;

public class StorageResolverFactory {
    private Map<String, StorageResolver> resolvers = new HashMap<String, StorageResolver>();
    
    public StorageResolverFactory() {
        resolvers.put("mast", new MastResolver());
        resolvers.put("gemini", new GeminiResolver());
        resolvers.put("ad", new AdResolver());
        resolvers.put("sdss", new SdssResolver());
        resolvers.put("noao", new NoaoResolver());
        resolvers.put("subaru", new SubaruResolver());
        resolvers.put("vos", new VOSpaceResolver());
        resolvers.put("xmm", new XmmResolver());
    }

    /**
     * Determines the correct resolver based on the URI Scheme.
     * @param artifactURI
     * @return The corresponding StorageResolver or null if no resolver is 
     *      found matching the scheme. 
     */
    public StorageResolver getStorageResolver(URI artifactURI) {
        StorageResolver resolver = resolvers.get(artifactURI.getScheme());
        
        return resolver;
    }
}
