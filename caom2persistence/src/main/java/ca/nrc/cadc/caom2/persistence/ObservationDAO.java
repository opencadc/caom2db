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
*  $Revision: 4 $
*
************************************************************************
*/

package ca.nrc.cadc.caom2.persistence;

import ca.nrc.cadc.caom2.Artifact;
import ca.nrc.cadc.caom2.CaomEntity;
import ca.nrc.cadc.caom2.Chunk;
import ca.nrc.cadc.caom2.Observation;
import ca.nrc.cadc.caom2.ObservationResponse;
import ca.nrc.cadc.caom2.ObservationState;
import ca.nrc.cadc.caom2.ObservationURI;
import ca.nrc.cadc.caom2.Part;
import ca.nrc.cadc.caom2.Plane;
import ca.nrc.cadc.caom2.persistence.skel.ArtifactSkeleton;
import ca.nrc.cadc.caom2.persistence.skel.ChunkSkeleton;
import ca.nrc.cadc.caom2.persistence.skel.ObservationSkeleton;
import ca.nrc.cadc.caom2.persistence.skel.PartSkeleton;
import ca.nrc.cadc.caom2.persistence.skel.PlaneSkeleton;
import ca.nrc.cadc.caom2.persistence.skel.Skeleton;
import ca.nrc.cadc.caom2.util.CaomValidator;

import java.util.ArrayList;
import java.util.Date;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import org.apache.log4j.Logger;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.dao.DataAccessException;

/**
 * Persistence layer operations.
 * 
 * @author pdowler
 */
public class ObservationDAO extends AbstractCaomEntityDAO<Observation>
{
    private static final Logger log = Logger.getLogger(ObservationDAO.class);

    private PlaneDAO planeDAO;
    
    public ObservationDAO() { }

    @Override
    public Map<String, Class> getParams()
    {
        Map<String,Class> ret = super.getParams();
        ret.put("schemaPrefixHack", Boolean.class);
        return ret;
    }

    @Override
    public void setConfig(Map<String,Object> config)
    {
        super.setConfig(config);
        this.planeDAO = new PlaneDAO(gen, forceUpdate, readOnly);
    }
    
    public boolean exists(ObservationURI uri)
    {
        Observation observation = get(uri, null, 1);
        return observation != null;
    }

    public UUID getID(ObservationURI uri) 
    {
        Observation observation = get(uri, null, 1);
        if (observation != null)
            return observation.getID();
        return null;
    }

    public ObservationURI getURI(UUID id) 
    {
        Observation observation = get(null, id, 1);
        if (observation != null)
            return observation.getURI();
        return null;
    }

    @Override
    public Observation get(UUID id)
    {
        if (id == null)
            throw new IllegalArgumentException("id cannot be null");
        return get(null, id, SQLGenerator.MAX_DEPTH);
    }

    /**
     * Get list of observation states in ascending order.
     * @param collection
     * @param minLastModified
     * @param maxLastModified
     * @param batchSize
     * @return 
     */
    public List<ObservationState> getObservationList(String collection, Date minLastModified, Date maxLastModified, Integer batchSize)
    {
        return getObservationList(collection, minLastModified, maxLastModified, batchSize, true);
    }
    
    /**
     * Get list of observation states in the specified timestamp order.
     * @param collection
     * @param minLastModified
     * @param maxLastModified
     * @param batchSize
     * @param ascendingOrder
     * @return 
     */
    public List<ObservationState> getObservationList(String collection, Date minLastModified, Date maxLastModified, 
            Integer batchSize, boolean ascendingOrder)
    {
        checkInit();
        log.debug("getObservationStates: " + collection + " " + batchSize);
        
        // input check since this is a string
        CaomValidator.assertValidPathComponent(ObservationDAO.class, "collection", collection);
        
        long t = System.currentTimeMillis();
        
        try
        {
            String sql = gen.getSelectSQL(ObservationState.class, minLastModified, maxLastModified, batchSize, ascendingOrder, collection);
            
            if (log.isDebugEnabled())
                log.debug("GET: " + Util.formatSQL(sql));

            JdbcTemplate jdbc = new JdbcTemplate(dataSource);
            List result = jdbc.query(sql, gen.getObservationStateMapper());
            return (List<ObservationState>) result;
        }
        finally
        {
            long dt = System.currentTimeMillis() - t;
            log.debug("getObservationStates: " + collection + " " + batchSize + " " + dt + "ms");
        }
    }
    
    // pdd: for harvester to get state  and (observation or error) reading single observations from db
    public List<ObservationResponse> getList(String collection, Date minLastModified, Date maxLastModified, Integer batchSize)
    {
        long t = System.currentTimeMillis();

        try
        {
            List<ObservationState> states = getObservationList(collection, minLastModified, maxLastModified, batchSize);
            List<ObservationResponse> ret = new ArrayList<ObservationResponse>(states.size());
            
            for (ObservationState s : states)
            {
                ObservationResponse r = new ObservationResponse(s);
                try
                {
                    r.observation = get(s.getURI());
                }
                catch(Exception ex)
                {
                    r.error = new IllegalStateException("failed to read " + s.getURI() + " from database", ex);
                }
                ret.add(r);
            }
            return ret;
        }
        finally
        {
            long dt = System.currentTimeMillis() - t;
            log.debug("getList: " + collection + " " + batchSize + " " + dt + "ms");
        }
    }
    // pdd: temporary hack for use in harvester retring skipped found in above getList impl
    public ObservationResponse getAlt(ObservationURI uri)
    {
        long t = System.currentTimeMillis();

        try
        {
            ObservationState s = new ObservationState(uri);
            ObservationResponse ret = new ObservationResponse(s);
            try
            {
                ret.observation = get(s.getURI());
                if (ret.observation == null) 
                    return null;
            }
            catch(Exception ex)
            {
                ret.error = new IllegalStateException("failed to read " + s.getURI() + " from database", ex);
            }
            return ret;
        }
        finally
        {
            long dt = System.currentTimeMillis() - t;
            log.debug("getAlt: " + uri + " " + dt + "ms");
        }
    }
    
    // pdd: for harvester to get just the observation object and check timestamps
    public Observation getShallow(UUID id)
    {
        if (id == null)
            throw new IllegalArgumentException("id cannot be null");
        return get(null, id, 1);
    }

    /**
     * Get list of observations to non-standard depth. This method will get observations (depth=1), 
     * planes (depth=2), etc. Values from 1 to SQLGenerator.MAX_DEPTH (5) are valid.
     * 
     * @param c
     * @param minlastModified
     * @param maxLastModified
     * @param batchSize
     * @param depth
     * @return 
     */
    @Override
    public List<Observation> getList(Class<Observation> c, Date minlastModified, Date maxLastModified, Integer batchSize, int depth)
    {
        return super.getList(c, minlastModified, maxLastModified, batchSize, depth);
    }
    
    /**
     * Get a stored observation by URI.
     *
     * @param uri
     * @return the complete observation
     */
    public Observation get(ObservationURI uri)
    {
        if (uri == null)
            throw new IllegalArgumentException("uri cannot be null");
        return get(uri, null, SQLGenerator.MAX_DEPTH);
    }

    private Observation get(ObservationURI uri, UUID id, int depth)
    {
        checkInit();
        if (uri == null && id == null)
            throw new IllegalArgumentException("args cannot be null");
        log.debug("GET: " + uri);
        long t = System.currentTimeMillis();

        try
        {
            String sql;
            if (uri != null)
                sql = gen.getSelectSQL(uri, depth);
            else
                sql = gen.getSelectSQL(id, depth, false);
            
            if (log.isDebugEnabled())
                log.debug("GET: " + Util.formatSQL(sql));

            JdbcTemplate jdbc = new JdbcTemplate(dataSource);
            Object result = jdbc.query(sql, gen.getObservationExtractor());
            if (result == null)
                return null;
            if (result instanceof List)
            {
                List obs = (List) result;
                if (obs.isEmpty())
                    return null;
                if (obs.size() > 1)
                    throw new RuntimeException("BUG: get " + uri + " query returned " + obs.size() + " observations");
                Object o = obs.get(0);
                if (o instanceof Observation)
                {
                    Observation ret = (Observation) obs.get(0);
                    return ret;
                }
                else
                    throw new RuntimeException("BUG: query returned an unexpected type " + o.getClass().getName());
            }
            throw new RuntimeException("BUG: query returned an unexpected type " + result.getClass().getName());
        }
        finally
        {
            long dt = System.currentTimeMillis() - t;
            log.debug("GET: " + uri + " " + dt + "ms");
        }
    }

    /**
     * Store an observation.
     * 
     * @param obs
     */
    public void put(Observation obs)
    {
        if (readOnly)
            throw new UnsupportedOperationException("put in readOnly mode");
        checkInit();
        if (obs == null)
            throw new IllegalArgumentException("arg cannot be null");
        log.debug("PUT: " + obs.getURI() + ", planes: " + obs.getPlanes().size());
        long t = System.currentTimeMillis();

        boolean txnOpen = false;
        try
        {
            log.debug("starting transaction");
            getTransactionManager().startTransaction();
            txnOpen = true;
            
            JdbcTemplate jdbc = new JdbcTemplate(dataSource);
            // NOTE: this is by ID which means to update the caller must get(uri) then put(o)
            //       and if they do not get(uri) they can get a duplicate observation error
            //       if they violate unique keys... but if it was by uri, it would be the same
            //       result as if they skipped the get(uri)
            String sql = gen.getSelectSQL(obs.getID(), SQLGenerator.MAX_DEPTH, true);
            log.debug("PUT: " + sql);
            ObservationSkeleton cur = (ObservationSkeleton) jdbc.query(sql, new ObservationSkeletonExtractor());

            // update metadata checksums, maybe modified timestamps
            boolean updateMax = updateEntity(obs, cur);
            
            // delete obsolete children
            List<Pair<Plane>> pairs = new ArrayList<Pair<Plane>>();
            if (cur != null)
            {
                // delete the skeletons that are not in obs.getPlanes()
                for (PlaneSkeleton ps : cur.planes)
                {
                    Plane p = Util.findPlane(obs.getPlanes(), ps.id);
                    if ( p == null ) // removed by client
                    {
                        log.info("PUT: caused delete: " + ps.id);
                        planeDAO.delete(ps, jdbc);
                    }
                }
                // pair up planes and skeletons for insert/update
                for (Plane p : obs.getPlanes())
                {
                    PlaneSkeleton ps = Util.findPlaneSkel(cur.planes, p.getID());
                    pairs.add(new Pair<Plane>(ps, p)); // null ok
                }
            }
            else
                for (Plane p : obs.getPlanes())
                    pairs.add(new Pair<Plane>(null, p));

            super.put(cur, obs, null, jdbc);

            // insert/update children
            LinkedList<CaomEntity> parents = new LinkedList<CaomEntity>();
            parents.push(obs);
            for (Pair<Plane> p : pairs)
                planeDAO.put(p.cur, p.val, parents, jdbc);
            
            log.debug("committing transaction");
            getTransactionManager().commitTransaction();
            log.debug("commit: OK");
            txnOpen = false;
        }
        catch(DataAccessException e)
        {
            log.debug("failed to insert " + obs + ": ", e);
            getTransactionManager().rollbackTransaction();
            log.debug("rollback: OK");
            txnOpen = false;
            throw e;
        }
        finally
        {
            if (txnOpen)
            {
                log.error("BUG - open transaction in finally");
                getTransactionManager().rollbackTransaction();
                log.error("rollback: OK");
            }
            long dt = System.currentTimeMillis() - t;
            log.debug("PUT: " + obs.getURI() + " " + dt + "ms");
        }
    }

    /**
     * Delete a stored observation by URI.
     *
     * @param uri
     */
    public void delete(ObservationURI uri)
    {
        if (uri == null)
            throw new IllegalArgumentException("uri arg cannot be null");
        deleteImpl(null, uri);
    }

    public void delete(UUID id)
    {
        if (id == null)
            throw new IllegalArgumentException("id arg cannot be null");
        deleteImpl(id, null);
    }

    private void deleteImpl(UUID id, ObservationURI uri)
    {
        if (readOnly)
            throw new UnsupportedOperationException("put in readOnly mode");
        checkInit();
        // null check in public methods above
        log.debug("DELETE: " + id);
        long t = System.currentTimeMillis();

        boolean txnOpen = false;
        try
        {
            log.debug("starting transaction");
            getTransactionManager().startTransaction();
            txnOpen = true;
            
            JdbcTemplate jdbc = new JdbcTemplate(dataSource);
            String sql = null;
            if (id != null)
                sql = gen.getSelectSQL(id, SQLGenerator.MAX_DEPTH, true);
            else
                sql = gen.getSelectSQL(uri, SQLGenerator.MAX_DEPTH, true);
            log.debug("DELETE: " + sql);
            ObservationSkeleton skel = (ObservationSkeleton) jdbc.query(sql, gen.getSkeletonExtractor(ObservationSkeleton.class));
            if (skel != null)
                delete(skel, jdbc);
            else
                log.debug("DELETE: not found: " + id);
            
            log.debug("committing transaction");
            getTransactionManager().commitTransaction();
            log.debug("commit: OK");
            txnOpen = false;
        }
        catch(DataAccessException e)
        {
            log.debug("failed to delete " + id + ": ", e);
            getTransactionManager().rollbackTransaction();
            log.debug("rollback: OK");
            txnOpen = false;
            throw e;
        }
        finally
        {
            if (txnOpen)
            {
                log.error("BUG - open transaction in finally");
                getTransactionManager().rollbackTransaction();
                log.error("rollback: OK");
            }
            long dt = System.currentTimeMillis() - t;
            log.debug("DELETE: " + id + " " + dt + "ms");
        }
    }
    
    @Override
    protected void deleteChildren(Skeleton s, JdbcTemplate jdbc)
    {
        ObservationSkeleton o = (ObservationSkeleton) s;
        if (o.planes.size() > 0)
        {
            // delete children of planes
            for (PlaneSkeleton p : o.planes)
                planeDAO.deleteChildren(p, jdbc);

            // delete planes by FK
            EntityDelete op = gen.getEntityDelete(Plane.class, false);
            op.setID(o.id);
            op.execute(jdbc);
            //String sql = gen.getDeleteSQL(Plane.class, o.id, false);
            //log.debug("delete: " + sql);
            //jdbc.update(sql);
        }
        else
            log.debug("no children: " + o.id);
    }

    // update CaomEntity state: 
    // always compute and assign: metaChecksum, accMetaChecksum
    // assign if metaChecksum changes: lastModified
    // assign if lastModified changed or a child's maxLastModified changes
    private boolean updateEntity(Observation entity, ObservationSkeleton s)
    {
        if (computeLastModified && s != null)
        {
            // keep timestamps from database
            Util.assignLastModified(entity, s.lastModified, "lastModified");
            Util.assignLastModified(entity, s.maxLastModified, "maxLastModified");
        }
        
        Date now = new Date();
        boolean updateMax = false;
        
        // check for added or modified
        for (Plane plane : entity.getPlanes())
        {
            PlaneSkeleton skel = null;
            if (s != null)
                for (PlaneSkeleton ss : s.planes)
                {
                    if (plane.getID().equals(ss.id))
                        skel = ss;
                }
            boolean ulm = updateEntity(plane, skel, now);
            updateMax = updateMax || ulm;
        }
        // check for deleted (unmatched skel)
        if (s != null)
        {
            for (PlaneSkeleton ss : s.planes)
            {
                Plane p = Util.findPlane(entity.getPlanes(), ss.id);
                boolean ulm = (p == null);
                updateMax = updateMax || ulm;
            }
        }

        // new or changed
        int nsc = entity.getStateCode();
        digest.reset(); // just in case
        Util.assignMetaChecksum(entity, entity.computeMetaChecksum(digest), "metaChecksum");
        Util.assignMetaChecksum(entity, entity.computeAccMetaChecksum(digest), "accMetaChecksum");
        
        boolean delta = false;
        if (s == null)
            delta = true;
        else if (s.metaChecksum != null)
            delta = !entity.getMetaChecksum().equals(s.metaChecksum);
        else
            delta = (s.stateCode != nsc); // fallback for null checksum in database
                
        if (computeLastModified && delta)        
        {
            Util.assignLastModified(entity, now, "lastModified");
            updateMax = true;
        }
        
        if (computeLastModified && updateMax)
            Util.assignLastModified(entity, now, "maxLastModified");

        return updateMax;
    }
    
    private boolean updateEntity(Plane entity, PlaneSkeleton s, Date now)
    {
        if (computeLastModified && s != null)
        {
            Util.assignLastModified(entity, s.lastModified, "lastModified");
            Util.assignLastModified(entity, s.maxLastModified, "maxLastModified");
        }
        
        boolean updateMax = false;
        for (Artifact artifact : entity.getArtifacts())
        {
            ArtifactSkeleton skel = null;
            if (s != null)
                for (ArtifactSkeleton ss : s.artifacts)
                {
                    if (artifact.getID().equals(ss.id))
                        skel = ss;
                }
            boolean ulm = updateEntity(artifact, skel, now);
            updateMax = updateMax || ulm;
        }
        // check for deleted (unmatched skel)
        if (s != null)
        {
            for (ArtifactSkeleton ss : s.artifacts)
            {
                Artifact a = Util.findArtifact(entity.getArtifacts(), ss.id);
                boolean ulm = (a == null);
                updateMax = updateMax || ulm;
            }
        }

        // new or changed
        int nsc = entity.getStateCode();
        digest.reset(); // just in case
        Util.assignMetaChecksum(entity, entity.computeMetaChecksum(digest), "metaChecksum");
        Util.assignMetaChecksum(entity, entity.computeAccMetaChecksum(digest), "accMetaChecksum");
        
        boolean delta = false;
        if (s == null)
            delta = true;
        else if (s.metaChecksum != null)
            delta = !entity.getMetaChecksum().equals(s.metaChecksum);
        else
            delta = (s.stateCode != nsc); // fallback
                
        if (computeLastModified && delta)        
        {
            Util.assignLastModified(entity, now, "lastModified");
            updateMax = true;
        }

        if (computeLastModified && updateMax)
            Util.assignLastModified(entity, now, "maxLastModified");

        return updateMax;
    }

    private boolean updateEntity(Artifact entity, ArtifactSkeleton s, Date now)
    {
        if (computeLastModified && s != null)
        {
            Util.assignLastModified(entity, s.lastModified, "lastModified");
            Util.assignLastModified(entity, s.maxLastModified, "maxLastModified");
        }

        boolean updateMax = false;
        for (Part part : entity.getParts())
        {
            PartSkeleton skel = null;
            if (s != null)
                for (PartSkeleton ss : s.parts)
                {
                    if (part.getID().equals(ss.id))
                        skel = ss;
                }
            boolean ulm = updateEntity(part, skel, now);
            updateMax = updateMax || ulm;
        }
        // check for deleted (unmatched skel)
        if (s != null)
        {
            for (PartSkeleton ss : s.parts)
            {
                Part p = Util.findPart(entity.getParts(), ss.id);
                boolean ulm = (p == null);
                updateMax = updateMax || ulm;
            }
        }
        
        // new or changed
        int nsc = entity.getStateCode();
        digest.reset(); // just in case
        Util.assignMetaChecksum(entity, entity.computeMetaChecksum(digest), "metaChecksum");
        Util.assignMetaChecksum(entity, entity.computeAccMetaChecksum(digest), "accMetaChecksum");
        
        boolean delta = false;
        if (s == null)
            delta = true;
        else if (s.metaChecksum != null)
            delta = !entity.getMetaChecksum().equals(s.metaChecksum);
        else
            delta = (s.stateCode != nsc); // fallback
                
        if (computeLastModified && delta)        
        {
            Util.assignLastModified(entity, now, "lastModified");
            updateMax = true;
        }

        if (computeLastModified && updateMax)
            Util.assignLastModified(entity, now, "maxLastModified");

        return updateMax;
    }

    private boolean updateEntity(Part entity, PartSkeleton s, Date now)
    {
        if (computeLastModified && s != null)
        {
            Util.assignLastModified(entity, s.lastModified, "lastModified");
            Util.assignLastModified(entity, s.maxLastModified, "maxLastModified");
        }

        boolean updateMax = false;
        for (Chunk chunk : entity.getChunks())
        {
            ChunkSkeleton skel = null;
            if (s != null)
                for (ChunkSkeleton ss : s.chunks)
                {
                    if (chunk.getID().equals(ss.id))
                        skel = ss;
                }
            boolean ulm = updateEntity(chunk, skel, now);
            updateMax = updateMax || ulm;
        }
        // check for deleted (unmatched skel)
        if (s != null)
        {
            for (ChunkSkeleton ss : s.chunks)
            {
                Chunk c = Util.findChunk(entity.getChunks(), ss.id);
                boolean ulm = (c == null);
                updateMax = updateMax || ulm;
            }
        }
        
        // new or changed
        int nsc = entity.getStateCode();
        digest.reset(); // just in case
        Util.assignMetaChecksum(entity, entity.computeMetaChecksum(digest), "metaChecksum");
        Util.assignMetaChecksum(entity, entity.computeAccMetaChecksum(digest), "accMetaChecksum");
        
        boolean delta = false;
        if (s == null)
            delta = true;
        else if (s.metaChecksum != null)
            delta = !entity.getMetaChecksum().equals(s.metaChecksum);
        else
            delta = (s.stateCode != nsc); // fallback
                
        if (computeLastModified && delta)        
        {
            Util.assignLastModified(entity, now, "lastModified");
            updateMax = true;
        }
        
        if (computeLastModified && updateMax)
            Util.assignLastModified(entity, now, "maxLastModified");

        return updateMax;
    }

    private boolean updateEntity(Chunk entity, ChunkSkeleton s, Date now)
    {
        if (computeLastModified && s != null)
        {
            Util.assignLastModified(entity, s.lastModified, "lastModified");
            Util.assignLastModified(entity, s.maxLastModified, "maxLastModified");
        }

        boolean updateMax = false;

        // new or changed
        int nsc = entity.getStateCode();
        digest.reset(); // just in case
        Util.assignMetaChecksum(entity, entity.computeMetaChecksum(digest), "metaChecksum");
        Util.assignMetaChecksum(entity, entity.computeAccMetaChecksum(digest), "accMetaChecksum");
        
        boolean delta = false;
        if (s == null)
            delta = true;
        else if (s.metaChecksum != null)
            delta = !entity.getMetaChecksum().equals(s.metaChecksum);
        else
            delta = (s.stateCode != nsc); // fallback
                
        if (computeLastModified && delta)        
        {
            Util.assignLastModified(entity, now, "lastModified");
            updateMax = true;
        }
        
        if (computeLastModified && updateMax)
            Util.assignLastModified(entity, now, "maxLastModified");

        return updateMax;
    }
}
