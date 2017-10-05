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

package ca.nrc.cadc.caom2.harvester;

import ca.nrc.cadc.caom2.DeletedObservation;
import ca.nrc.cadc.caom2.DeletedObservationMetaReadAccess;
import ca.nrc.cadc.caom2.DeletedPlaneDataReadAccess;
import ca.nrc.cadc.caom2.DeletedPlaneMetaReadAccess;
import ca.nrc.cadc.caom2.access.ObservationMetaReadAccess;
import ca.nrc.cadc.caom2.access.PlaneDataReadAccess;
import ca.nrc.cadc.caom2.access.PlaneMetaReadAccess;
import ca.nrc.cadc.caom2.harvester.state.HarvestState;
import ca.nrc.cadc.caom2.version.InitDatabase;
import ca.nrc.cadc.db.ConnectionConfig;
import ca.nrc.cadc.db.DBConfig;
import ca.nrc.cadc.db.DBUtil;
import java.io.IOException;
import java.net.URISyntaxException;
import java.util.Date;
import javax.sql.DataSource;
import org.apache.log4j.Logger;

/**
 * A wrapper that calls the Harvester implementations in the right order.
 *
 * @author pdowler
 */
public class CaomHarvester implements Runnable {

    /**
     * log
     */
    private static Logger log = Logger.getLogger(CaomHarvester.class);
    /**
     * initdb
     */
    private InitDatabase initdb;
    /**
     * obsHarvester
     */
    private ObservationHarvester obsHarvester;
    /**
     * obsDeleter
     */
    private DeletionHarvester obsDeleter;
    /**
     * observationMetaHarvester
     */
    private ReadAccessHarvester observationMetaHarvester;
    /**
     * planeDataHarvester
     */
    private ReadAccessHarvester planeDataHarvester;
    /**
     * planeMetaHarvester
     */
    private ReadAccessHarvester planeMetaHarvester;
    /**
     * observationMetaDeleter
     */
    private DeletionHarvester observationMetaDeleter;
    /**
     * planeDataDeleter
     */
    private DeletionHarvester planeDataDeleter;
    /**
     * planeDataDeleter
     */
    private DeletionHarvester planeMetaDeleter;

    /**
     * Harvest everything.
     *
     * @param dryrun
     * true if no changed in the data base are applied during the process
     * @param nochecksum
     * disable metadata checksum comparison
     * @param compute
     * compute plane metadata from WCS before insert
     * @param src
     * source server,database,schema
     * @param dest
     * destination server,database,schema
     * @param batchSize
     * number of observations per batch (~memory consumption)
     * @param batchFactor
     * multiplier for batchSize when harvesting single-table entities
     * @param full
     * full harvest of all source entities
     * @param skip
     * flag that indicates if shipped observations should be dealt
     * @param maxDate
     * latest date to be using during harvester
     * @param nthreads max threads when harvesting from a service
     * @throws java.io.IOException
     * IOException
     * @throws URISyntaxException
     * URISyntaxException
     */
    public CaomHarvester(boolean dryrun, boolean nochecksum, boolean compute,
            HarvestResource src, HarvestResource dest,
            int batchSize, int batchFactor, boolean full, boolean skip, Date maxDate, int nthreads)
            throws IOException, URISyntaxException {
        Integer entityBatchSize = batchSize * batchFactor;

        DBConfig dbrc = new DBConfig();
        ConnectionConfig cc = dbrc.getConnectionConfig(dest.getDatabaseServer(), dest.getDatabase());
        DataSource ds = DBUtil.getDataSource(cc);
        this.initdb = new InitDatabase(ds, dest.getDatabase(), dest.getSchema());

        this.obsHarvester = new ObservationHarvester(src, dest, batchSize, full, dryrun, nochecksum, nthreads);
        obsHarvester.setSkipped(skip);
        obsHarvester.setMaxDate(maxDate);
        obsHarvester.setComputePlaneMetadata(compute);

        boolean extendedFeatures = src.getDatabaseServer() != null;
        if (extendedFeatures) {
            if (src.getHarvestAC()) {
                this.observationMetaHarvester = new ReadAccessHarvester(ObservationMetaReadAccess.class, src, dest, entityBatchSize, full, dryrun);
                observationMetaHarvester.setSkipped(skip);
                this.planeDataHarvester = new ReadAccessHarvester(PlaneDataReadAccess.class, src, dest, entityBatchSize, full, dryrun);
                planeDataHarvester.setSkipped(skip);
                this.planeMetaHarvester = new ReadAccessHarvester(PlaneMetaReadAccess.class, src, dest, entityBatchSize, full, dryrun);
                planeMetaHarvester.setSkipped(skip);
            }

            // deletions in incremental mode only
            if (!full && !skip) {
                this.obsDeleter = new DeletionHarvester(DeletedObservation.class, src, dest, entityBatchSize, dryrun);

                if (src.getHarvestAC()) {
                    this.observationMetaDeleter = new DeletionHarvester(DeletedObservationMetaReadAccess.class, src, dest, entityBatchSize, dryrun);
                    this.planeMetaDeleter = new DeletionHarvester(DeletedPlaneMetaReadAccess.class, src, dest, entityBatchSize, dryrun);
                    this.planeDataDeleter = new DeletionHarvester(DeletedPlaneDataReadAccess.class, src, dest, entityBatchSize, dryrun);
                }
            }
        }
        log.info("     source: " + src.getIdentifier());
        log.info("destination: " + dest.getIdentifier());
    }

    /**
     * run
     */
    @Override
    public void run() {

        if (obsHarvester.getComputePlaneMetadata()) {
            // make sure wcslib can be loaded
            try {
                log.info("loading ca.nrc.cadc.wcs.WCSLib");
                Class.forName("ca.nrc.cadc.wcs.WCSLib");
            } catch (Throwable t) {
                throw new RuntimeException("FATAL - failed to load WCSLib JNI binding", t);
            }
        }

        boolean init = false;
        if (initdb != null) {
            boolean created = initdb.doInit();
            if (created) {
                init = true; // database is empty so can bypass processing old
            }                             // deletions
        }

        // clean up old access control tuples before harvest to avoid conflicts
        // from delete+create
        if (observationMetaDeleter != null) {
            boolean initDel = init;
            if (!init) {
                // check if we have ever harvested before
                HarvestState hs = observationMetaHarvester.harvestState.get(observationMetaHarvester.source, observationMetaHarvester.cname);
                initDel = (hs.curID == null && hs.curLastModified == null); // never harvested
            }
            observationMetaDeleter.setInitHarvestState(initDel);
            observationMetaDeleter.run();
            log.info("init: " + observationMetaDeleter.cname);
        }
        if (planeDataDeleter != null) {
            boolean initDel = init;
            if (!init) {
                // check if we have ever harvested before
                HarvestState hs = planeDataHarvester.harvestState.get(planeDataHarvester.source, planeDataHarvester.cname);
                initDel = (hs.curID == null && hs.curLastModified == null); // never harvested
            }
            planeDataDeleter.setInitHarvestState(initDel);
            planeDataDeleter.run();
            log.info("init: " + planeDataDeleter.cname);
        }
        if (planeMetaDeleter != null) {
            boolean initDel = init;
            if (!init) {
                // check if we have ever harvested before
                HarvestState hs = planeMetaHarvester.harvestState.get(planeMetaHarvester.source, planeMetaHarvester.cname);
                initDel = (hs.curID == null && hs.curLastModified == null); // never harvested
            }
            planeMetaDeleter.setInitHarvestState(initDel);
            planeMetaDeleter.run();
            log.info("init: " + planeMetaDeleter.cname);
        }

        // delete observations before harvest to avoid observationURI conflicts
        // from delete+create
        if (obsDeleter != null) {
            boolean initDel = init;
            if (!init) {
                // check if we have ever harvested before
                HarvestState hs = obsHarvester.harvestState.get(obsHarvester.source, obsHarvester.cname);
                initDel = (hs.curID == null && hs.curLastModified == null); // never harvested
            }
            log.info("init: " + obsDeleter.source + " " + obsDeleter.cname);
            obsDeleter.setInitHarvestState(initDel);
            obsDeleter.run();
        }

        // harvest observations
        if (obsHarvester != null) {
            log.debug("************** obsHarvester.run() ***************");
            obsHarvester.run();
        }

        // make sure access control tuples are harvested after observations
        // because they update asset tables and fail if asset is missing
        if (observationMetaHarvester != null) {
            observationMetaHarvester.run();
        }
        if (planeDataHarvester != null) {
            planeDataHarvester.run();
        }
        if (planeMetaHarvester != null) {
            planeMetaHarvester.run();
        }

    }
}
