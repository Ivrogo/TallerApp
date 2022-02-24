/*
 * Classe que defineix un taller. Un taller es defineix pel seu CIF, nom, adreça, 
 * un vector de mecànics i mecàniques, un vector de clients i clientes, un vector de recanvis 
 * i un vector de reparacions. 
 */
package model;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;
import principal.Component;
import principal.GestorTallerMecanicException;

/**
 *
 * @author fta
 */
public class Taller implements Component, Serializable {

    private String cif;
    private String nom;
    private String adreca;
    private List<Component> components = new ArrayList();

    public Taller(String cif, String nom, String adreça) {
        this.cif = cif;
        this.nom = nom;
        this.adreca = adreça;
    }


    public String getCif() {
        return cif;
    }

    public void setCif(String cif) {
        this.cif = cif;
    }

    public String getNom() {
        return nom;
    }

    public void setNom(String nom) {
        this.nom = nom;
    }

    public String getAdreca() {
        return adreca;
    }

    public void setAdreca(String adreca) {
        this.adreca = adreca;
    }

    public List<Component> getComponents() {
        return components;
    }

    public void setComponents(List<Component> components) {
        this.components = components;
    }
    

    public static Taller addTaller() {

        String cif, nom, adreca;

        System.out.println("\nCIF del taller:");
        cif = DADES.next();
        DADES.nextLine(); //Neteja buffer
        System.out.println("\nNom del taller:");
        nom = DADES.nextLine();
        System.out.println("\nAdreça del taller:");
        adreca = DADES.nextLine();

        return new Taller(cif, nom, adreca);
    }


    public void updateComponent() {
        System.out.println("\nCIF del taller: " + cif);
        System.out.println("\nEntra el nou CIF:");
        cif = DADES.next();
        DADES.nextLine(); //Neteja buffer
        System.out.println("\nNom del taller: " + nom);
        System.out.println("\nEntra el nou nom:");
        nom = DADES.nextLine();
        System.out.println("\nAdreça del taller: " + adreca);
        System.out.println("\nEntra la nova adreça:");
        adreca = DADES.nextLine();
    }

  
    public void showComponent() {
        System.out.println("\nEl nom del taller amb CIF " + cif + " és:");
        System.out.println("\nNom del taller: " + nom);
        System.out.println("\nAdreça del taller: " + adreca);
    }


    public void addClient(Client client) throws GestorTallerMecanicException {

        if (client == null) {
            client = Client.addClient();
        }

        if (selectComponent(1, client.getNif()) == -1) {
            components.add(client);
        } else {
            throw new GestorTallerMecanicException("5");
        }
    }
    

    public void addMecanic(Mecanic mecanic) throws GestorTallerMecanicException {

        if (mecanic == null) {
            mecanic = Mecanic.addMecanic();
        }

        if (selectComponent(2, mecanic.getNif()) == -1) {
            components.add(mecanic);
        } else {
            throw new GestorTallerMecanicException("6");
        }
    }


    public void addRecanvi(Recanvi recanvi) throws GestorTallerMecanicException {

        if (recanvi == null) {
            recanvi = Recanvi.addRecanvi();
        }

        if (selectComponent(3, recanvi.getCodi()) == -1) {
            components.add(recanvi);
        } else {
            throw new GestorTallerMecanicException("7");
        }
    }


    public void addReparacio(Reparacio reparacio) throws GestorTallerMecanicException {

        if (reparacio == null) {
            reparacio = Reparacio.addReparacio();
        }

        if (selectComponent(4, reparacio.getCodi()) == -1) {
            components.add(reparacio);
        } else {
            throw new GestorTallerMecanicException("8");
        }
    }

    
    public int selectComponent(int tipusComponent, String id) {

        if (id == null) {
            //Demanem quin tipus de component vol seleccionar i el seu identificador (id)
            switch (tipusComponent) {
                case 1:
                    System.out.println("NIF del client o de la clienta?:");
                    break;
                case 2:
                    System.out.println("NIF del mecànic o la mecànica?:");
                    break;
                case 3:
                    System.out.println("Codi del recanvi?:");
                    break;
                case 4:
                    System.out.println("Codi de la reparació?:");
                    break;
            }

            id = DADES.next();
        }

        int posElement = -1; //Posició que ocupa el component seleccionat dins el vector de components del taller

        //Seleccionem la posició que ocupa el component dins el vector de components
        // del taller
        for (int i = 0; i < components.size(); i++) {
            if (components.get(i) instanceof Client && tipusComponent == 1) {
                if (((Client) components.get(i)).getNif().equals(id)) {
                    return i;
                }
            } else if (components.get(i) instanceof Mecanic && tipusComponent == 2) {
                if (((Mecanic) components.get(i)).getNif().equals(id)) {
                    return i;
                }
            } else if (components.get(i) instanceof Recanvi && tipusComponent == 3) {
                if (((Recanvi) components.get(i)).getCodi().equals(id)) {
                    return i;
                }
            } else if (components.get(i) instanceof Reparacio && tipusComponent == 4) {
                if (((Reparacio) components.get(i)).getCodi().equals(id)) {
                    return i;
                }
            }
        }

        return posElement;
    }

    public void addClientReparacio() {

        Reparacio reparacioSel;
        int pos = selectComponent(4, null);

        if (pos >= 0) {

            reparacioSel = (Reparacio)this.getComponents().get(pos);

            pos = selectComponent(1, null);

            if (pos >= 0) {
                reparacioSel.setClient((Client)this.getComponents().get(pos));
            } else {
                System.out.println("\nNo existeix aquest cient o clienta");
            }

        } else {
            System.out.println("\nNo existeix aquesta reparació");
        }

    }

    public void addMecanicReparacio() throws GestorTallerMecanicException {

        Reparacio reparacioSel;
        int pos = selectComponent(4, null);

        if (pos >= 0) {

            reparacioSel = (Reparacio)this.getComponents().get(pos);

            pos = selectComponent(2, null);

            if (pos >= 0) {
                reparacioSel.addMecanic((Mecanic)this.getComponents().get(pos));
            } else {
                System.out.println("\nNo existeix aquest mecànic o mecànica");
            }

        } else {
            System.out.println("\nNo existeix aquesta reparació");
        }

    }

    public void addRecanviReparacio() throws GestorTallerMecanicException {

        Reparacio reparacioSel;
        int pos = selectComponent(4, null);

        if (pos >= 0) {

            reparacioSel = (Reparacio)this.getComponents().get(pos);

            pos = selectComponent(3, null);

            if (pos >= 0) {

                if (!((Recanvi)this.getComponents().get(pos)).isAssignat()) {
                    reparacioSel.addRecanvi((Recanvi)this.getComponents().get(pos));
                    ((Recanvi)getComponents().get(pos)).setAssignat(true);
                } else {
                    System.out.println("\nEl recanvi ja ha estat assignat");
                }

            } else {
                System.out.println("\nNo existeix aquest recanvi");
            }

        } else {
            System.out.println("\nNo existeix aquesta reparació");
        }

    }

    public void calcularPreuReparacio() {
        Reparacio reparacioSel;
        int pos = selectComponent(4, null);

        if (pos >= 0) {

            reparacioSel = (Reparacio)this.getComponents().get(pos);

            System.out.println("\nQuantes hores s'han invertit en la reparació?");
            reparacioSel.addPreu(DADES.nextInt());

        } else {
            System.out.println("\nNo existeix aquesta reparació");
        }
    }
}
