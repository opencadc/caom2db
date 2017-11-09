package ca.nrc.cadc.caom2.repo.client.transform;

import ca.nrc.cadc.caom2.ObservationState;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.net.URISyntaxException;
import java.text.DateFormat;
import java.text.ParseException;
import java.util.Comparator;
import java.util.List;

public abstract class Transformer {

    private DateFormat dateFormat = null;
    private char separator = '0';
    private char endOfLine = '0';

    private Comparator<ObservationState> maxLasModifiedComparator = new Comparator<ObservationState>() {
        @Override
        public int compare(ObservationState o1, ObservationState o2) {
            return o1.maxLastModified.compareTo(o2.maxLastModified);
        }
    };

    public abstract List<ObservationState> transform(final ByteArrayOutputStream bos) throws ParseException, IOException, URISyntaxException;

    public Transformer(DateFormat dateFormat, char separator, char endOfLine) {
        this.dateFormat = dateFormat;
        this.separator = separator;
        this.endOfLine = endOfLine;
    }

    public Comparator<ObservationState> getComparator() {
        return maxLasModifiedComparator;
    }

    public DateFormat getDateFormat() {
        return dateFormat;
    }

    public char getSeparator() {
        return separator;
    }

    public char getEndOfLine() {
        return endOfLine;
    }
}
