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

import java.io.IOException;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import java.util.ListIterator;
import java.util.Map;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;

import javax.sql.DataSource;

import org.apache.log4j.Logger;

import ca.nrc.cadc.caom2.Artifact;
import ca.nrc.cadc.caom2.Observation;
import ca.nrc.cadc.caom2.ObservationState;
import ca.nrc.cadc.caom2.ObservationURI;
import ca.nrc.cadc.caom2.Plane;
import ca.nrc.cadc.caom2.harvester.Harvester;
import ca.nrc.cadc.caom2.harvester.SkippedWrapperURI;
import ca.nrc.cadc.caom2.harvester.state.HarvestSkipURI;
import ca.nrc.cadc.caom2.harvester.state.HarvestSkipURIDAO;
import ca.nrc.cadc.caom2.harvester.state.HarvestStateDAO;
import ca.nrc.cadc.caom2.harvester.state.PostgresqlHarvestStateDAO;
import ca.nrc.cadc.caom2.persistence.DatabaseObservationDAO;

public class ArtifactHarvester extends Harvester
{

    private static final Integer SKIPDAO_BATCH_SIZE = Integer.valueOf(1000);

    private static final Logger log = Logger.getLogger(ArtifactHarvester.class);

    private DatabaseObservationDAO destObservationDAO;

    // private ArtifactDAO artifactDAO;
    private ArtifactStore artifactStore;
    private HarvestStateDAO harvestStateDAO;
    private HarvestSkipURIDAO harvestSkip;

    private boolean dryrun;
    private boolean full;
    private Date startDate;
    private boolean firstIteration = true;
    private Integer batchSize = null;
    private Date maxDate;
    private int nthreads = 1;
    private String collection;

    public ArtifactHarvester(String collection, /* ArtifactDAO artifactDAO, */ String[] dbInfo, ArtifactStore artifactStore, boolean dryrun, int nthreads, boolean full)
    {
        // this.artifactDAO = artifactDAO;
        this.artifactStore = artifactStore;
        this.dryrun = dryrun;
        this.full = full;
        this.nthreads = nthreads;
        this.collection = collection;

        this.harvestStateDAO = new PostgresqlHarvestStateDAO(
                /* artifactDAO.getDatasource() */null, dbInfo[1], dbInfo[2]);
        this.harvestSkip = new HarvestSkipURIDAO(
                /* artifactDAO.getDatasource() */null, dbInfo[1], dbInfo[2], SKIPDAO_BATCH_SIZE);
    }

    private void init() throws IOException
    {
        Map<String, Object> config2 = getConfigDAO(dest);
        this.destObservationDAO = new DatabaseObservationDAO();
        destObservationDAO.setConfig(config2);
        // destObservationDAO.setComputeLastModified(false); // copy as-is
        initHarvestState(destObservationDAO.getDataSource(), Observation.class);
    }

    @Override
    public void run()
    {
        try
        {
            init();
            // TODO: Use the ArtifactDAO to find artifacts with
            // maxLastModified > last run.

            // TODO: for each, use the ArtifactStore to see if it already
            // has the artifact.

            // TODO: save the ones that are missing using the HarvestSkipURIDAO

            log.info("START \n");

            doit();

            log.info("DONE \n");

        }
        catch (IOException e)
        {
            e.printStackTrace();
        }
    }

    private Progress doit()
    {
        Progress ret = new Progress();

        long t = System.currentTimeMillis();
        long tState = -1;
        long tQuery = -1;
        long tTransaction = -1;

        try
        {
            System.gc(); // hint
            t = System.currentTimeMillis();

            tState = System.currentTimeMillis() - t;
            t = System.currentTimeMillis();

            List<Callable<Boolean>> tasks = new ArrayList<Callable<Boolean>>();

            if (!full) // search in skipped table
            {
                List<ObservationState> partialList = destObservationDAO.getObservationList(collection, null, null, null);

                if (partialList != null && !partialList.isEmpty())
                {
                    log.info("found: " + partialList.size());

                    ListIterator<ObservationState> iter1 = partialList.listIterator();

                    while (iter1.hasNext())
                    {
                        ObservationState os = iter1.next();
                        Observation o = destObservationDAO.get(os.getURI());
                        // os.maxLastModified;
                        iter1.remove(); // allow garbage collection during loop

                        if (o != null)
                        {
                            for (Plane p : o.getPlanes())
                            {
                                for (Artifact artifact : p.getArtifacts())
                                {
                                    tasks.add(new StoreProcessor(artifact, artifactStore));
                                }
                            }
                        }
                    }
                }
            }
            else // full sync
            {
                List<Observation> fullList = destObservationDAO.getList(Observation.class, null, null, null);
                if (fullList != null && !fullList.isEmpty())
                {
                    for (Observation o : fullList)
                    {
                        for (Plane p : o.getPlanes())
                        {
                            for (Artifact artifact : p.getArtifacts())
                            {
                                tasks.add(new StoreProcessor(artifact, artifactStore));
                            }
                        }
                    }
                }
            }
            ExecutorService taskExecutor = null;
            try
            {
                // Run tasks in a fixed thread pool
                taskExecutor = Executors.newFixedThreadPool(nthreads);
                List<Future<Boolean>> futures;

                futures = taskExecutor.invokeAll(tasks);

                for (Future<Boolean> f : futures)
                {
                    Boolean res = f.get();
                }
            }
            catch (InterruptedException | ExecutionException e)
            {
                log.error("Error when executing thread in ThreadPool: " + e.getMessage() + " caused by: " + e.getCause().toString());
            }
            finally
            {
                if (taskExecutor != null)
                {
                    taskExecutor.shutdown();
                }
            }

        }
        finally
        {
            tTransaction = System.currentTimeMillis() - t;
            log.debug("time to get HarvestState: " + tState + "ms");
            log.debug("time to run ObservationListQuery: " + tQuery + "ms");
            log.debug("time to run transactions: " + tTransaction + "ms");
        }
        return ret;
    }

    private List<SkippedWrapperURI<Observation>> getSkipped(Date start)
    {
        log.info("harvest window (skip): " + format(start) + " [" + batchSize + "]" + " source = " + source + " cname = " + cname);
        List<HarvestSkipURI> skip = harvestSkip.get(source, cname, start);

        List<SkippedWrapperURI<Observation>> ret = new ArrayList<SkippedWrapperURI<Observation>>(skip.size());
        for (HarvestSkipURI hs : skip)
        {
            log.info("hs.getSkipID(): " + hs.getSkipID());
            log.info("start: " + start);

            Observation obs = destObservationDAO.get(new ObservationURI(hs.getSkipID()));

            if (obs != null)
                ret.add(new SkippedWrapperURI<Observation>(obs, hs));
        }
        return ret;
    }

    @Override
    protected void initHarvestState(DataSource ds, Class c)
    {
        super.initHarvestState(ds, c);
        this.harvestSkip = new HarvestSkipURIDAO(ds, dest[1], dest[2], batchSize);
    }

    private static class Progress
    {

        boolean done = false;
        boolean abort = false;
        int found = 0;
        int ingested = 0;
        int failed = 0;
        int handled = 0;

        @Override
        public String toString()
        {
            return found + " ingested: " + ingested + " failed: " + failed;
        }
    }

    private class StoreProcessor implements Callable<Boolean>
    {
        private Artifact artifact = null;
        private ArtifactStore artifactStore = null;

        public StoreProcessor(Artifact a, ArtifactStore as)
        {
            this.artifact = a;
            this.artifactStore = as;
        }

        private Boolean processArtifact()
        {
            return true;
        }

        @Override
        public Boolean call() throws Exception
        {
            return processArtifact();
        }
    }
}
