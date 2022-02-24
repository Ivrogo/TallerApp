package persistencia;

import model.Client;
import model.Mecanic;
import model.Recanvi;
import java.io.File;
import javax.xml.parsers.DocumentBuilder;
import javax.xml.parsers.DocumentBuilderFactory;
import javax.xml.parsers.ParserConfigurationException;
import javax.xml.transform.Transformer;
import javax.xml.transform.TransformerFactory;
import javax.xml.transform.dom.DOMSource;
import javax.xml.transform.stream.StreamResult;
import org.w3c.dom.Document;
import org.w3c.dom.Element;
import org.w3c.dom.Node;
import org.w3c.dom.NodeList;
import principal.GestorTallerMecanicException;
import model.Reparacio;
import model.Taller;

/**
 *
 * @author fta
 */
public class GestorXML implements ProveedorPersistencia {

    private Document doc;
    private Taller taller;

    public Document getDoc() {
        return doc;
    }

    public Taller getTaller() {
        return taller;
    }

    public void desarTaller(String nomFitxer, Taller taller) throws GestorTallerMecanicException {
        construeixModel(taller);
        desarModel(nomFitxer);
    }

    public void carregarTaller(String nomFitxer) throws GestorTallerMecanicException {
        carregarFitxer(nomFitxer);
        llegirFitxerTaller();
    }

    /*Paràmetres: Taller a partir de la qual volem construir el model
     *
     *Acció: 
     * - Llegir els atributs de l'objecte taller passat per paràmetre 
     *   per construir un model (document XML) sobre el Document doc (atribut de GestorXML).
     *
     * - L'arrel del document XML és "taller". Aquesta arrel, l'heu d'afegir 
     *   a doc. Un cop fet això, heu de recórrer l'ArrayList components de Taller
     *   i per a cada component, afegir un fill a doc. Cada fill tindrà com atributs
     *   els atributs de l'objecte (codi, nom, nif, etc.)
     *
     * - Si es tracta d'un recanvi, quan està assignat hem d'assignar a l'atribut que representa
     *   dins el document XML si està assignat o no, el valor "Si", i en cas contrari el valor "No"
     *   
     * - Si es tracta d'una reparació, a més, heu d'afegir fills addicionals amb el client, els
     *   mecànics i mecàniques i els recanvis.
     *
     * - Si heu de gestionar alguna excepció relacionada amb la construcció del model,
     *   heu de llençar una excepció de tipus GestorTallerMecanicException amb codi 
     *   "GestorXML.model".
     *
     *Retorn: cap
     */
    public void construeixModel(Taller taller) throws GestorTallerMecanicException {
        //Es construeix el document model
        DocumentBuilderFactory builderFactory = DocumentBuilderFactory.newInstance();
        DocumentBuilder builder;
        Element fill, net;

        try {
            builder = builderFactory.newDocumentBuilder();
        } catch (ParserConfigurationException ex) {
            throw new GestorTallerMecanicException("GestorXML.model");
        }

        this.doc = (Document) builder.newDocument();

        //Element arrel
        Element arrel = doc.createElement("taller");
        arrel.setAttribute("cif", taller.getCif());
        arrel.setAttribute("nom", taller.getNom());
        arrel.setAttribute("adreca", taller.getAdreca());
        doc.appendChild(arrel);

        for (int i = 0; i < taller.getComponents().size(); i++) {

            if (taller.getComponents().get(i) instanceof Client) {

                fill = doc.createElement("client");

                fill.setAttribute("nif", ((Client) taller.getComponents().get(i)).getNif());
                fill.setAttribute("nom", ((Client) taller.getComponents().get(i)).getNom());
                fill.setAttribute("telefon", ((Client) taller.getComponents().get(i)).getTelefon());
                fill.setAttribute("correu", ((Client) taller.getComponents().get(i)).getCorreu());

                arrel.appendChild(fill);

            } else if (taller.getComponents().get(i) instanceof Mecanic) {

                fill = doc.createElement("mecanic");

                fill.setAttribute("nif", ((Mecanic) taller.getComponents().get(i)).getNif());
                fill.setAttribute("nom", ((Mecanic) taller.getComponents().get(i)).getNom());
                fill.setAttribute("telefon", ((Mecanic) taller.getComponents().get(i)).getTelefon());
                fill.setAttribute("correu", ((Mecanic) taller.getComponents().get(i)).getCorreu());

                arrel.appendChild(fill);

            } else if (taller.getComponents().get(i) instanceof Recanvi) {

                fill = doc.createElement("recanvi");

                fill.setAttribute("codi", ((Recanvi) taller.getComponents().get(i)).getCodi());
                fill.setAttribute("nom", ((Recanvi) taller.getComponents().get(i)).getNom());
                fill.setAttribute("fabricant", ((Recanvi) taller.getComponents().get(i)).getFabricant());
                fill.setAttribute("preu", String.valueOf(((Recanvi)taller.getComponents().get(i)).getPreu()));
                
                if (((Recanvi) taller.getComponents().get(i)).isAssignat()) {
                    fill.setAttribute("assignat", "Si");
                } else {
                    fill.setAttribute("assignat", "No");
                }

                arrel.appendChild(fill);

            } else if (taller.getComponents().get(i) instanceof Reparacio) {

                fill = doc.createElement("reparacio");

                fill.setAttribute("codi", ((Reparacio) taller.getComponents().get(i)).getCodi());
                fill.setAttribute("dataInici", ((Reparacio) taller.getComponents().get(i)).getDataInici());
                fill.setAttribute("dataFi", ((Reparacio) taller.getComponents().get(i)).getDataFi());                
                fill.setAttribute("preu", String.valueOf(((Reparacio)taller.getComponents().get(i)).getPreu()));
                
                net = doc.createElement("client");
                net.setAttribute("nif", ((Client) ((Reparacio) taller.getComponents().get(i)).getClient()).getNif());
                net.setAttribute("nom", ((Client) ((Reparacio) taller.getComponents().get(i)).getClient()).getNom());
                net.setAttribute("telefon", ((Client) ((Reparacio) taller.getComponents().get(i)).getClient()).getTelefon());
                net.setAttribute("correu", ((Client) ((Reparacio) taller.getComponents().get(i)).getClient()).getCorreu());

                fill.appendChild(net);

                for (int j = 0; j < ((Reparacio) taller.getComponents().get(i)).getElements().get("mecanics").size(); j++) {

                    net = doc.createElement("mecanic");

                    net.setAttribute("nif", ((Mecanic) ((Reparacio) taller.getComponents().get(i)).getElements().get("mecanics").get(j)).getNif());
                    net.setAttribute("nom", ((Mecanic) ((Reparacio) taller.getComponents().get(i)).getElements().get("mecanics").get(j)).getNom());
                    net.setAttribute("telefon", ((Mecanic) ((Reparacio) taller.getComponents().get(i)).getElements().get("mecanics").get(j)).getTelefon());
                    net.setAttribute("correu", ((Mecanic) ((Reparacio) taller.getComponents().get(i)).getElements().get("mecanics").get(j)).getCorreu());

                    fill.appendChild(net);

                }
                
                for (int j = 0; j < ((Reparacio) taller.getComponents().get(i)).getElements().get("recanvis").size(); j++) {

                    net = doc.createElement("recanvi");

                    net.setAttribute("codi", ((Recanvi) ((Reparacio) taller.getComponents().get(i)).getElements().get("recanvis").get(j)).getCodi());
                    net.setAttribute("nom", ((Recanvi) ((Reparacio) taller.getComponents().get(i)).getElements().get("recanvis").get(j)).getNom());
                    net.setAttribute("fabricant", ((Recanvi) ((Reparacio) taller.getComponents().get(i)).getElements().get("recanvis").get(j)).getFabricant());
                    net.setAttribute("preu", String.valueOf(((Recanvi) ((Reparacio) taller.getComponents().get(i)).getElements().get("recanvis").get(j)).getPreu()));
                
                    if (((Recanvi) ((Reparacio) taller.getComponents().get(i)).getElements().get("recanvis").get(j)).isAssignat()) {
                        net.setAttribute("assignat", "Si");
                    } else {
                        net.setAttribute("assignat", "No");
                    }

                    fill.appendChild(net);

                }

                arrel.appendChild(fill);
            }
        }
    }

    public void desarModel(String nomFitxer) throws GestorTallerMecanicException {
        try {
            TransformerFactory tf = TransformerFactory.newInstance();
            Transformer transformer = tf.newTransformer();
            DOMSource source = new DOMSource(doc);
            File f = new File(nomFitxer + ".xml");
            StreamResult result = new StreamResult(f);
            transformer.transform(source, result);
        } catch (Exception ex) {
            throw new GestorTallerMecanicException("GestorXML.desar");
        }
    }

    public void carregarFitxer(String nomFitxer) throws GestorTallerMecanicException {
        DocumentBuilderFactory builderFactory = DocumentBuilderFactory.newInstance();
        try {
            DocumentBuilder builder = builderFactory.newDocumentBuilder();
            File f = new File(nomFitxer + ".xml");
            doc = builder.parse(f);
        } catch (Exception ex) {
            throw new GestorTallerMecanicException("GestorXML.carrega");
        }
    }

    /*Paràmetres: cap
     *
     *Acció: 
     * - Llegir el fitxer del vostre sistema i carregar-lo sobre l'atribut doc, 
     *   per assignar valors als atributs de Taller i la resta d'objectes que formen 
     *   els components de Taller.
     *    
     * - Primer heu de crear l'objecte de la classe Taller a partir de l'arrel del document XML
     *   per després recórrer el doc i per cada fill, afegir un objecte a l'atribut 
     *   components de Taller mitjançant el mètode escaient de la classe Taller. Recordeu
     *   que com l'arrel conté els atributs CIF, nom i adreça del taller, al fer 
     *   Element arrel = doc.getDocumentElement(); ja podeu crear l'objecte de la classe.
     *
     * - Si l'element que s'ha llegit és un recanvi, penseu que els valors "Si" i "No", s'han de guardar
     *   com boleans quan l'afegiu a components del taller o a una reparació.
     *
     * - Si l'element del document XML que s'ha llegit és una reparació, recordeu que a 
     *   més d'afegir-lo a components, també haureu d'assignar-li el client, mecànic i mecàniques
     *   i els recanvis.
     *
     *Retorn: cap
     */
    private void llegirFitxerTaller() throws GestorTallerMecanicException {
        String component;

        Element arrel = doc.getDocumentElement();

        //Es crea l'objecte de la classe Taller
        taller = new Taller(arrel.getAttribute("cif"), arrel.getAttribute("nom"), arrel.getAttribute("adreca"));

        //Recorregut de nodes fill d'un element       
        NodeList llistaFills = arrel.getChildNodes();

        for (int i = 0; i < llistaFills.getLength(); i++) {

            Node fill = llistaFills.item(i);

            if (fill.getNodeType() == Node.ELEMENT_NODE) {

                component = fill.getNodeName();

                if (component.equals("client")) {

                    String nif = ((Element) fill).getAttribute("nif");
                    String nom = ((Element) fill).getAttribute("nom");
                    String telefon = ((Element) fill).getAttribute("telefon");
                    String correu = ((Element) fill).getAttribute("correu");
                    
                    taller.addClient(new Client(nif, nom, correu, telefon));

                } else if (component.equals("mecanic")) {
                    
                    String nif = ((Element) fill).getAttribute("nif");
                    String nom = ((Element) fill).getAttribute("nom");
                    String telefon = ((Element) fill).getAttribute("telefon");
                    String correu = ((Element) fill).getAttribute("correu");
                    
                    taller.addMecanic(new Mecanic(nif, nom, correu, telefon));
                    
                } else if (component.equals("recanvi")){ 

                    String codi = ((Element) fill).getAttribute("codi");
                    String nom = ((Element) fill).getAttribute("nom");
                    String fabricant = ((Element) fill).getAttribute("fabricant");
                    double preu = Double.parseDouble(((Element) fill).getAttribute("preu"));
                    String tipus = ((Element) fill).getAttribute("nom");   
                    
                    boolean assignat=false;
                    
                    if(((Element) fill).getAttribute("assignat").equals("Si")){
                        assignat=true;
                    }

                    taller.addRecanvi(new Recanvi(codi, nom, fabricant, preu, assignat));

                } else if (component.equals("reparacio")) {

                    String codi = ((Element) fill).getAttribute("codi");
                    String dataInici = ((Element) fill).getAttribute("dataInici");
                    String dataFi = ((Element) fill).getAttribute("dataFi");
                    double preu = Double.parseDouble(((Element) fill).getAttribute("preu"));                   
                                      
                    Reparacio novaReparacio = new Reparacio(codi, dataInici, dataFi);                  
                    

                    taller.addReparacio(novaReparacio);

                    NodeList nets = fill.getChildNodes();

                    for (int j = 0; j < nets.getLength(); j++) {

                        Node net = nets.item(j);

                        if (net != null && net.getNodeType() == Node.ELEMENT_NODE) {

                            component = net.getNodeName();

                            switch (component) {
                                case "client":
                                    novaReparacio.setClient((Client) taller.getComponents().get(taller.selectComponent(1, ((Element) net).getAttribute("nif"))));
                                    break;
                                case "mecanic":
                                    novaReparacio.addMecanic((Mecanic) taller.getComponents().get(taller.selectComponent(2, ((Element) net).getAttribute("nif"))));
                                    break;
                                default: //recanvi
                                    novaReparacio.addRecanvi((Recanvi) taller.getComponents().get(taller.selectComponent(3, ((Element) net).getAttribute("codi"))));
                                    break;
                            }

                        }
                    }
                }
            }
        }
    }
}
