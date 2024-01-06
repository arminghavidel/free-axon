package query;

import java.io.Serializable;

public class RocketResponse implements Serializable {
    public String response;

    public String getResponse() {
        return response;
    }

    public void setResponse(String response) {
        this.response = response;
    }
}
