/*
************************************************************************
*******************  CANADIAN ASTRONOMY DATA CENTRE  *******************
**************  CENTRE CANADIEN DE DONNÉES ASTRONOMIQUES  **************
*
*  (c) 2018.                            (c) 2018.
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

import ca.nrc.cadc.caom2.persistence.ObservationDAO;
import ca.nrc.cadc.util.ArgumentMap;

import java.util.ArrayList;
import java.util.List;

import javax.security.auth.Subject;

import org.apache.log4j.Logger;

/**
 * Class to support the 'discover' mode.
 *
 * @author majorb, yeunga
 */
public class Discover extends DiscoverOrDownload {

    private static Logger log = Logger.getLogger(Discover.class);
    private String collection = null;
    private ArtifactHarvester harvester = null;

    public Discover(ArgumentMap am) {
        super(am);

        if (!this.done) {
            this.collection = this.parseCollection(am);
            if (!this.done) {
                ObservationDAO observationDAO = new ObservationDAO();
                observationDAO.setConfig(this.daoConfig);

                this.harvester = new ArtifactHarvester(
                        observationDAO, dbInfo, artifactStore, collection, this.batchSize);
            }
        }
    }

    public void printUsage() {
        StringBuilder sb = new StringBuilder();
        sb.append("\n\nusage: ").append(this.applicationName).append(" [mode-args]");
        sb.append("\n\n    [mode-args]:");
        sb.append("\n        --database=<server.database.schema>");
        sb.append("\n        --collection=<collection> : The collection to discover");
        sb.append("\n        --batchsize=<integer> Max observations to check (default: 1000)");
        sb.append("\n        --continue : Repeat batches until no work left");
        sb.append("\n\n    optional general args:");
        sb.append("\n        -v | --verbose");
        sb.append("\n        -d | --debug");
        sb.append("\n        -h | --help");
        sb.append("\n        --profile : Profile task execution");
        sb.append("\n\n    authentication:");
        sb.append("\n        [--netrc|--cert=<pem file>]");
        sb.append("\n        --netrc : read username and password(s) from ~/.netrc file");
        sb.append("\n        --cert=<pem file> : read client certificate from PEM file");

        log.warn(sb.toString());    
        this.done = true;
    }
    
    public void execute() throws Exception {
        if (!this.done) {
            this.setExitValue(2);
            List<ShutdownListener> listeners = new ArrayList<ShutdownListener>(2);
            listeners.add(harvester);
            Runtime.getRuntime().addShutdownHook(new Thread(new ShutdownHook(listeners)));
            super.execute();
    	}
    }
    
    protected boolean executeCommand() throws Exception {
        boolean stopHarvest = false;
        if (this.subject != null) {
            stopHarvest = Subject.doAs(this.subject, harvester) == 0;
        } else {
            stopHarvest = harvester.run() == 0;
        }
        return stopHarvest;
    }
}