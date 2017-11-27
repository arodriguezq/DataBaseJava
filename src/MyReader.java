import org.xml.sax.SAXException;

import javax.xml.parsers.ParserConfigurationException;
import java.io.IOException;
import java.util.List;

public class MyReader extends DomReader {

    public MyReader(String xml) throws ParserConfigurationException, SAXException, IOException {
        super(xml);
    }

    public List<String> listCountry() {
        return super.extractList("/world/country/name/text()");
    }

    public List<String> listCity(String country) {
        return super.extractList("/world/country[name='" + country + "']//city/name[1]/text()");
    }
}
