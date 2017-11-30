package xPath;

import org.w3c.dom.Document;
import org.w3c.dom.NodeList;
import org.xml.sax.SAXException;

import javax.xml.parsers.DocumentBuilder;
import javax.xml.parsers.DocumentBuilderFactory;
import javax.xml.parsers.ParserConfigurationException;
import javax.xml.xpath.XPath;
import javax.xml.xpath.XPathConstants;
import javax.xml.xpath.XPathExpressionException;
import javax.xml.xpath.XPathFactory;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

/**
 * Copyright 2015 Daniel Cruz <i>
 * <a href="mailto:dani.cruz.morales@gmail.com">dani.cruz.morales@gmail.com</a><br><br>
 * <p>
 * This is free software, licensed under the GNU General Public License v3.<br>
 * See <a href="http://www.gnu.org/licenses/gpl.html">http://www.gnu.org/licenses/gpl.html</a> for more information.
 *
 * @author Daniel Cruz
 */
public class DomReader {

    private DocumentBuilderFactory factory;
    private DocumentBuilder builder;
    private Document doc;
    private XPath xpath;

    public DomReader(String xml) throws ParserConfigurationException, SAXException, IOException {
        factory = DocumentBuilderFactory.newInstance();
        factory.setNamespaceAware(true);

        doc = null;

        builder = factory.newDocumentBuilder();
        doc = builder.parse(xml);

        XPathFactory xpathFactory = XPathFactory.newInstance();

        xpath = xpathFactory.newXPath();

    }

    protected String extractValue(String exp) {
        String name = null;
        try {
            name = (String) (xpath.compile(exp)).evaluate(doc, XPathConstants.STRING);
        } catch (XPathExpressionException e) {
            e.printStackTrace();
        }

        return name;
    }

    protected List<String> extractList(String exp) {
        ArrayList<String> out = new ArrayList<>();
        NodeList nodes;

        try {

            nodes = (NodeList) (xpath.compile(exp)).evaluate(doc, XPathConstants.NODESET);
            for (int i = 0; i < nodes.getLength(); i++)
                out.add(nodes.item(i).getNodeValue());
        } catch (XPathExpressionException e) {

            e.printStackTrace();
        }

        return out;
    }

    protected Set<String> extractSet(String exp) {
        Set<String> out = new HashSet<>();
        NodeList nodes;

        try {

            nodes = (NodeList) (xpath.compile(exp)).evaluate(doc, XPathConstants.NODESET);
            for (int i = 0; i < nodes.getLength(); i++)
                out.add(nodes.item(i).getNodeValue());
        } catch (XPathExpressionException e) {

            e.printStackTrace();
        }

        return out;
    }

    final static String nl = System.getProperty("line.separator");

    public static void print(List<String> list) {

        StringBuffer b = new StringBuffer();
        for (String string : list) {
            b.append(string);
            b.append(nl);
        }
        System.out.println(b);

    }

}
