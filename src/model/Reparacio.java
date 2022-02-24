/*
 * Classe que defineix una reparació. Una reparació es defineix pel seu codi identificador,
 * la data d'inici i de fi de la reparació en format dd/mm/aaaa, on dd és el dia, mm és el 
 * mes i aaaa és l'any, el preu de la reparació, el client o clienta que ha demanat la reparació, un
 * vector amb els mecànics i mecàniques que s'encarreguen de la reparació i un vector amb els recanvis fets 
 * servir en la reparació.
 */
package model;

import model.Client;
import model.Mecanic;
import model.Recanvi;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;
import principal.Component;
import principal.GestorTallerMecanicException;

/**
 *
 * @author fta
 */
public class Reparacio implements Component{

    private String codi;
    private String dataInici;
    private String dataFi;
    private double preu;
    private Client client; 
    private Map<String, ArrayList> elements = new HashMap();

    public Reparacio(String codi, String dataInici, String dataFi) {
        this.codi = codi;
        this.dataInici = dataInici;
        this.dataFi = dataFi;
        elements.put("mecanics", new ArrayList<Mecanic>());
        elements.put("recanvis", new ArrayList<Recanvi>());
    }

    public String getCodi() {
        return codi;
    }

    public void setCodi(String codi) {
        this.codi = codi;
    }

    public String getDataInici() {
        return dataInici;
    }

    public void setDataInici(String dataInici) {
        this.dataInici = dataInici;
    }

    public String getDataFi() {
        return dataFi;
    }

    public void setDataFi(String dataFi) {
        this.dataFi = dataFi;
    }

    public double getPreu() {
        return preu;
    }

    public void setPreu(double preu) {
        this.preu = preu;
    }

    public Client getClient() {
        return client;
    }

    public void setClient(Client client) {
        this.client = client;
    }

    public Map<String, ArrayList> getElements() {
        return elements;
    }

    public void setElements(Map<String, ArrayList> elements) {
        this.elements = elements;
    }


    public static Reparacio addReparacio() {

        String codi, dataInici, dataFi;

        System.out.println("\nCodi identificador de la reparació:");
        codi = DADES.next();
        System.out.println("\nData d'incic de la reparació:");
        dataInici = DADES.next();
        System.out.println("\nData de fi de la reparació:");
        dataFi = DADES.next();

        return new Reparacio(codi, dataInici, dataFi);
    }


    public void updateComponent() {
        System.out.println("\nCodi identificador de la reparació: " + codi);
        System.out.println("\nEntra el nou codi identificador:");
        codi = DADES.next();
        System.out.println("\nData d'incic de la reparació: " + dataInici);
        System.out.println("\nEntra la nova data d'inici:");
        dataInici = DADES.next();
        System.out.println("\nData de fi de la reparació: " + dataFi);
        System.out.println("\nEntra la nova data de fi:");
        dataFi = DADES.next();
    }


    public void addMecanic(Mecanic mecanic) throws GestorTallerMecanicException {

        for (int i = 0; i < elements.get("mecanics").size(); i++) {
            if (((Mecanic)elements.get("mecanics").get(i)).getNif().equals(mecanic.getNif())) {
                throw new GestorTallerMecanicException("3");
            }
        }

        elements.get("mecanics").add(mecanic);

    }


    public void addRecanvi(Recanvi recanvi) throws GestorTallerMecanicException {

        for (int i = 0; i < elements.get("recanvis").size(); i++) {
            if (((Recanvi)elements.get("recanvis").get(i)).getCodi().equals(recanvi.getCodi())) {
                throw new GestorTallerMecanicException("4");
            }
        }

        elements.get("recanvis").add(recanvi);
        
    }


    public void addPreu(int totalHores) {

        double preuRecanvis = 0;

        for (int i = 0; i < elements.get("recanvis").size(); i++) {
            preuRecanvis += ((Recanvi)elements.get("recanvis").get(i)).getPreu();
        }

        preu = preuRecanvis + 60.5 * totalHores;
    }

    public void showComponent() {
        System.out.println("\nLes dades de la reparació amb codi identificador " + codi + " són:");

        System.out.println("\nData d'inici: " + dataInici);

        System.out.println("\nData de fi: " + dataFi);

        System.out.println("\nPreu: " + preu);

        if (client != null) {
            System.out.println("\nClient: ");
            client.showComponent();
        }

        for (int i = 0; i < elements.get("mecanics").size(); i++) {
            ((Mecanic)elements.get("mecanics").get(i)).showComponent();
        }

        for (int i = 0; i < elements.get("recanvis").size(); i++) {
            ((Recanvi)elements.get("recanvis").get(i)).showComponent();
        }
    }

}
