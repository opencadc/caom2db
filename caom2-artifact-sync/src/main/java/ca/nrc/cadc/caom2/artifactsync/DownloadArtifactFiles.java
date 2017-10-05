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

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.MalformedURLException;
import java.net.URI;
import java.net.URL;
import java.security.PrivilegedExceptionAction;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;

import org.apache.log4j.Logger;

import ca.nrc.cadc.caom2.harvester.state.HarvestSkipURI;
import ca.nrc.cadc.caom2.harvester.state.HarvestSkipURIDAO;
import ca.nrc.cadc.caom2.persistence.ArtifactDAO;
import ca.nrc.cadc.io.ByteCountInputStream;
import ca.nrc.cadc.net.HttpDownload;
import ca.nrc.cadc.net.InputStreamWrapper;

public class DownloadArtifactFiles implements PrivilegedExceptionAction<Object>
{

    private static final Logger log = Logger.getLogger(DownloadArtifactFiles.class);
    private static final String MAST_BASE_ARTIFACT_URL = "https://masttest.stsci.edu/partners/download/file";

    private ArtifactStore artifactStore;
    private HarvestSkipURIDAO harvestSkipURIDAO;
    private String source;
    private int threads;

    public DownloadArtifactFiles(ArtifactDAO artifactDAO, String[] dbInfo, ArtifactStore artifactStore, int threads, int batchSize)
    {
        this.artifactStore = artifactStore;

        this.source = dbInfo[0] + "." + dbInfo[1] + "." + dbInfo[2];
        this.harvestSkipURIDAO = new HarvestSkipURIDAO(artifactDAO.getDataSource(), dbInfo[1], dbInfo[2], batchSize);

        this.threads = threads;
    }

    @Override
    public Object run() throws Exception
    {

        Date nullDate = null;
        List<HarvestSkipURI> artifacts = harvestSkipURIDAO.get(source, ArtifactHarvester.STATE_CLASS, nullDate);
        ExecutorService executor = Executors.newFixedThreadPool(threads);

        List<Callable<ArtifactDownloadResult>> tasks = new ArrayList<Callable<ArtifactDownloadResult>>();
        for (HarvestSkipURI skip : artifacts)
        {
            ArtifactDownloader downloader = new ArtifactDownloader(skip, artifactStore, harvestSkipURIDAO);
            tasks.add(downloader);
        }

        try
        {
            final long start = System.currentTimeMillis();
            List<Future<ArtifactDownloadResult>> results = executor.invokeAll(tasks);
            long successes = 0;
            long totalElapsedTime = 0;
            long totalBytes = 0;
            for (Future<ArtifactDownloadResult> f : results)
            {
                ArtifactDownloadResult result = f.get();
                if (result.success)
                {
                    successes++;
                    totalElapsedTime += result.elapsedTimeMillis;
                    totalBytes += result.bytesTransferred;
                }
            }
            final long end = System.currentTimeMillis() - start;

            double successRate = 0;
            if (results.size() > 0)
            {
                successRate = (double) successes / results.size();
            }
            long totalSeconds = totalElapsedTime / 1000;
            long bytesPerSecond = 0;
            if (totalSeconds > 0)
            {
                bytesPerSecond = totalBytes / totalSeconds;
            }
            double mbps = (double) bytesPerSecond / 125000;

            log.info("[batch-summary] Completed " + results.size() + " artifact download attempts");
            log.info("[batch-summary]     Successful file downloads: " + successes);
            log.info("[batch-summary]     Failed file downloads: " + (results.size() - successes));
            log.info("[batch-summary]     Success rate: " + successRate);
            log.info("[batch-summary]     Batch elapsed time (ms): " + end);
            log.info("[batch-summary]     Download threads: " + threads);
            log.info("[batch-summary]     Total bytes transferred: " + totalBytes);
            log.info("[batch-summary]     Data transfer time (ms): " + totalElapsedTime);
            log.info("[batch-summary]     Bytes/Second: " + bytesPerSecond + " (" + mbps + " Mbps)");

            StringBuilder endMessage = new StringBuilder();
            endMessage.append("END: {");
            endMessage.append("\"successCount\":\"").append(successes).append("\"");;
            endMessage.append(",");
            endMessage.append("\"failureCount\":\"").append(results.size() - successes).append("\"");
            endMessage.append(",");
            endMessage.append("\"time\":\"").append(totalElapsedTime).append("\"");
            endMessage.append(",");
            endMessage.append("\"bytes\":\"").append(totalBytes).append("\"");
            endMessage.append(",");
            endMessage.append("\"threads\":\"").append(threads).append("\"");
            endMessage.append("}");
            log.info(endMessage.toString());

        }
        catch (InterruptedException e)
        {
            log.info("Thread pool interupted", e);
        }
        catch (ExecutionException e)
        {
            log.error("Thread execution error", e);
        }

        return null;
    }

    private URL getSourceURL(URI artifactURI) throws MalformedURLException
    {
        String artifact = artifactURI.getSchemeSpecificPart();
        return new URL(MAST_BASE_ARTIFACT_URL + "/" + artifact);
    }

    class ArtifactDownloader implements Callable<ArtifactDownloadResult>, InputStreamWrapper
    {

        HarvestSkipURI skip;
        ArtifactStore artifactStore;
        HarvestSkipURIDAO harvestSkipURIDAO;
        boolean uploadSuccess = true;
        String uploadErrorMessage;
        long bytesTransferred;

        URI sourceChecksum;
        Long sourceLength;

        ArtifactDownloader(HarvestSkipURI skip, ArtifactStore artifactStore, HarvestSkipURIDAO harvestSkipURIDAO)
        {
            this.skip = skip;
            this.artifactStore = artifactStore;
            this.harvestSkipURIDAO = harvestSkipURIDAO;
        }

        @Override
        public ArtifactDownloadResult call() throws Exception
        {
            URI artifactURI = skip.getSkipID();
            URL url = getSourceURL(artifactURI);

            ArtifactDownloadResult result = new ArtifactDownloadResult(artifactURI);
            result.success = false;

            String threadName = Thread.currentThread().getName();

            try
            {
                // get the md5 and contentLength of the artifact
                OutputStream out = new ByteArrayOutputStream();
                HttpDownload head = new HttpDownload(url, out);
                head.setHeadOnly(true);
                head.run();
                int respCode = head.getResponseCode();

                if (head.getThrowable() != null || respCode != 200)
                {
                    StringBuilder sb = new StringBuilder("(" + respCode + ") ");
                    if (head.getThrowable() != null)
                    {
                        sb.append(head.getThrowable().getMessage());
                        if (log.isDebugEnabled())
                        {
                            log.error("[" + threadName + "] error determining artifact checksum: " + sb.toString(), head.getThrowable());
                        }
                        else
                        {
                            log.error("[" + threadName + "] error determining artifact checksum: " + sb.toString());
                        }

                    }
                    result.errorMessage = sb.toString();
                    return result;
                }

                String md5String = head.getContentMD5();
                log.debug("MAST content MD5: " + md5String);
                if (md5String != null)
                {
                    sourceChecksum = URI.create("MD5:" + md5String);
                }

                long contentLength = head.getContentLength();
                sourceLength = null;
                if (contentLength >= 0)
                {
                    sourceLength = new Long(contentLength);
                }

                // check again to be sure the destination doesn't already have
                // it
                if (artifactStore.contains(artifactURI, sourceChecksum))
                {
                    log.info("[" + threadName + "] ArtifactStore already has correct copy of " + artifactURI + " with checksum " + sourceChecksum);
                    result.success = true;
                    return result;
                }

                HttpDownload download = new HttpDownload(url, this);

                log.info("[" + threadName + "] Starting download of " + artifactURI + " from " + url);
                long start = System.currentTimeMillis();
                download.run();
                log.info("[" + threadName + "] Completed download of " + artifactURI + " from " + url);
                result.elapsedTimeMillis = System.currentTimeMillis() - start;

                respCode = download.getResponseCode();
                log.debug("Download response code: " + respCode);

                if (download.getThrowable() != null || respCode != 200)
                {
                    StringBuilder sb = new StringBuilder("(" + respCode + ") ");
                    if (download.getThrowable() != null)
                    {
                        sb.append(download.getThrowable().getMessage());
                        log.error("[" + threadName + "] error downloading artifact", download.getThrowable());
                    }
                    result.errorMessage = sb.toString();
                }

                if (uploadSuccess)
                {
                    result.success = true;
                }
                else
                {
                    result.errorMessage = uploadErrorMessage;
                }

                return result;
            }
            finally
            {
                // Update the skip table
                try
                {
                    if (result.success)
                    {
                        result.bytesTransferred = bytesTransferred;
                        harvestSkipURIDAO.delete(skip);
                    }
                    else
                    {
                        skip.errorMessage = result.errorMessage;
                        harvestSkipURIDAO.put(skip);
                    }
                }
                catch (Throwable t)
                {
                    log.error("[" + threadName + "] Failed to update or delete from skip table", t);
                }
            }
        }

        @Override
        public void read(InputStream inputStream) throws IOException
        {
            String threadName = Thread.currentThread().getName();
            URI artifactURI = skip.getSkipID();
            ByteCountInputStream byteCounter = new ByteCountInputStream(inputStream);
            try
            {
                log.info("[" + threadName + "] Starting upload of " + artifactURI);
                artifactStore.store(artifactURI, sourceChecksum, sourceLength, byteCounter);
                log.info("[" + threadName + "] Completed upload of " + artifactURI);
            }
            catch (Throwable t)
            {
                uploadSuccess = false;
                log.info("[" + threadName + "] Failed to upload " + artifactURI, t);
                uploadErrorMessage = "error uploading artifact: " + t.getMessage();
            }
            finally
            {
                if (byteCounter != null)
                {
                    bytesTransferred = byteCounter.getByteCount();
                }
            }
        }
    }

    class ArtifactDownloadResult
    {
        URI artifactURI;
        boolean success;
        String errorMessage;
        long elapsedTimeMillis;
        long bytesTransferred = 0;

        ArtifactDownloadResult(URI artifactURI)
        {
            this.artifactURI = artifactURI;
        }
    }

}