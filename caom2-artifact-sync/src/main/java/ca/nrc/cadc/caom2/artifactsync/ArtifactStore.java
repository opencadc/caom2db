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

import ca.nrc.cadc.net.TransientException;

import java.io.InputStream;
import java.net.URI;
import java.security.AccessControlException;

/**
 * An interface to a CAOM2 artifact storage system.
 *
 * @author majorb
 */
public interface ArtifactStore {

    /**
     * Checks for artifact existence.
     *
     * @param artifactURI
     *            The artifact identifier.
     * @param checksum
     *            The checksum of the artifact.
     * @return True in the artifact exists with the given checksum.
     *
     * @throws UnsupportedOperationException
     *             If the artifact uri cannot be resolved.
     * @throws UnsupportedOperationException
     *             If the checksum algorith is not supported.
     * @throws IllegalArgumentException
     *             If an aspect of the artifact uri is incorrect.
     * @throws AccessControlException
     *             If the calling user is not allowed to perform the query.
     * @throws TransientException
     *             If an unexpected runtime error occurs.
     * @throws RuntimeException
     *             If an unrecovarable error occurs.
     */
    public boolean contains(URI artifactURI, URI checksum)
            throws TransientException, UnsupportedOperationException, IllegalArgumentException, AccessControlException, IllegalStateException;

    /**
     * Saves an artifact. The artifact will be replaced if artifact already
     * exists with a different checksum.
     *
     * @param artifactURI
     *            The artifact identifier.
     * @param checksum
     *            The checksum of the artifact (can be null).
     * @param contentLength
     *            The size of the artifact in bytes (can be null).
     * @param data
     *            The artifact data.
     *
     * @throws UnsupportedOperationException
     *             If the artifact uri cannot be resolved.
     * @throws UnsupportedOperationException
     *             If the checksum algorith is not supported.
     * @throws IllegalArgumentException
     *             If an aspect of the artifact uri is incorrect.
     * @throws AccessControlException
     *             If the calling user is not allowed to upload the artifact.
     * @throws IllegalStateException
     *             If the artifact already exists.
     * @throws TransientException
     *             If an unexpected runtime error occurs.
     * @throws RuntimeException
     *             If an unrecovarable error occurs.
     */
    public void store(URI artifactURI, URI checksum, Long contentLength, InputStream data)
            throws TransientException, UnsupportedOperationException, IllegalArgumentException, AccessControlException, IllegalStateException;

}
