/*
************************************************************************
*******************  CANADIAN ASTRONOMY DATA CENTRE  *******************
**************  CENTRE CANADIEN DE DONNÉES ASTRONOMIQUES  **************
*
*  (c) 2011.                            (c) 2011.
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

package ca.nrc.cadc.caom2.repo.action;

import ca.nrc.cadc.caom2.ObservationState;
import ca.nrc.cadc.caom2.ObservationURI;
import ca.nrc.cadc.caom2.persistence.ObservationDAO;
import ca.nrc.cadc.io.ByteCountWriter;

import java.util.Calendar;
import java.util.Date;
import java.util.List;

import org.apache.log4j.Logger;

import com.csvreader.CsvWriter;

/**
 *
 * @author pdowler
 */
public class ListAction extends RepoAction
{   
    private static final Logger log = Logger.getLogger(ListAction.class);
	private Integer maxRec;
	private Date start;
	private Date end;
	
    public ListAction(Integer maxRec, Date start, Date end) 
    { 
    	this.maxRec = maxRec;
    	this.start = start;
    	this.end = end;
    }

    @Override
    public void doAction()
        throws Exception
    {
        ObservationURI uri = getURI();
        log.debug("START: " + uri);

        checkReadPermission(uri);

        ObservationDAO dao = getDAO();
        List<ObservationState> states = dao.getObservationList(
        		uri.getCollection(), this.start, this.end, this.maxRec);

        if (states == null)
            throw new ObservationNotFoundException(uri);

        // write with default schema        
        syncOutput.setHeader("Content-Type", "text/csv");
        ByteCountWriter bc = new ByteCountWriter(syncOutput.getWriter());
        CsvWriter writer = new CsvWriter(bc, ',');
        Calendar calendar = Calendar.getInstance();
        for (ObservationState state : states)
        {
        	writer.write(state.getObservationID());
        	calendar.setTime(state.getMaxLastModified());
        	String y = Integer.toString(calendar.get(Calendar.YEAR));
        	String mon = Integer.toString(calendar.get(Calendar.MONTH));
        	String d = Integer.toString(calendar.get(Calendar.DAY_OF_MONTH));
        	String h = Integer.toString(calendar.get(Calendar.HOUR));
        	String m = Integer.toString(calendar.get(Calendar.MINUTE));
        	String s = Integer.toString(calendar.get(Calendar.SECOND));
        	String ms = Integer.toString(calendar.get(Calendar.MILLISECOND));
        	writer.write(y + "-" + mon +"-" + d + " " + h + ":" + m + ":" + s + "." + ms);
        	writer.endRecord();
        }
        
        // ow.write(obs, bc);
        logInfo.setBytes(bc.getByteCount());
        
        log.debug("DONE: " + uri);
    }
}