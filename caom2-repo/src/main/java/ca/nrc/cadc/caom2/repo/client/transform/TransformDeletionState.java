package ca.nrc.cadc.caom2.repo.client.transform;

import ca.nrc.cadc.caom2.ObservationState;
import ca.nrc.cadc.caom2.ObservationURI;
import ca.nrc.cadc.date.DateUtil;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.net.URISyntaxException;
import java.text.DateFormat;
import java.text.ParseException;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;

import org.apache.log4j.Logger;

public class TransformDeletionState extends Transformer {

    private static final Logger log = Logger.getLogger(TransformDeletionState.class);

    public TransformDeletionState(DateFormat dateFormat, char separator, char endOfLine) {
        super(dateFormat, separator, endOfLine);
    }

    @Override
    public List<ObservationState> transform(ByteArrayOutputStream bos) throws ParseException, IOException, URISyntaxException {
        // <Observation.id> <Observation.collection> <Observation.observationID> <timestamp>
        List<ObservationState> list = new ArrayList<>();
        log.debug("********* " + bos.toString());

        String id = null;
        String sdate;
        Date date = null;
        String collection = null;

        String aux = "";

        boolean readingDate = false;
        boolean readingCollection = false;
        boolean readingId = false;
        boolean readingFirstValue = true; //first value (<Observation.id>) to be ignored

        for (int i = 0; i < bos.toString().length(); i++) {
            char c = bos.toString().charAt(i);

            if (c != getSeparator() && c != getEndOfLine()) {
                aux += c;
            } else if (c == getSeparator()) {
                if (readingFirstValue) {
                    readingFirstValue = false;
                    readingCollection = true;
                    readingId = false;
                    readingDate = false;
                    aux = "";
                } else if (readingCollection) {
                    collection = aux;
                    // log.debug("*************** collection: " + collection);
                    readingFirstValue = false;
                    readingCollection = false;
                    readingId = true;
                    readingDate = false;
                    aux = "";
                } else if (readingId) {
                    id = aux;
                    // log.debug("*************** id: " + id);
                    readingFirstValue = false;
                    readingCollection = false;
                    readingId = false;
                    readingDate = true;
                    aux = "";
                } else if (readingDate) {
                    sdate = aux;
                    // log.debug("*************** sdate: " + sdate);
                    date = DateUtil.flexToDate(sdate, getDateFormat());
                    readingFirstValue = false;
                    readingCollection = false;
                    readingId = false;
                    readingDate = false;
                    aux = "";
                }

            } else if (c == getEndOfLine()) {
                if (id == null || collection == null) {
                    continue;
                }

                ObservationState os = new ObservationState(new ObservationURI(collection, id));

                if (date == null) {
                    sdate = aux;
                    date = DateUtil.flexToDate(sdate, getDateFormat());
                }

                os.maxLastModified = date;

                list.add(os);
                readingCollection = false;
                readingId = false;
                readingFirstValue = true;
                readingDate = false;

            }
        }

        return list;

    }

}
