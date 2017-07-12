package ca.nrc.cadc.caom2.harvester;

import ca.nrc.cadc.caom2.Observation;

public class ObservationError {

    private Observation obs = null;
    private String error = null;

    public ObservationError(Observation o, String e) {
        obs = o;
        error = e;
    }

    public Observation getObs() {
        return obs;
    }

    public String getError() {
        return error;
    }

    @Override
    public String toString() {
        String res = "null";
        if (obs != null && error != null) {
            res = obs.getURI().getURI() + ": " + error;
        }
        return res;
    }
}
