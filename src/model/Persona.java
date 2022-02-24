
package model;

import principal.Component;

/**
 *
 * @author cesca
 */
public abstract class Persona implements Component {
    
    private String nif;
    private String nom;
    private String telefon;
    private String correu;

    
    public Persona(String nif, String nom, String telefon, String correu) {
        this.nif = nif;
        this.nom = nom;
        this.telefon = telefon;
        this.correu = correu;
    }


    public String getNif() {
        return nif;
    }

    public void setNif(String nif) {
        this.nif = nif;
    }

    public String getNom() {
        return nom;
    }

    public void setNom(String nom) {
        this.nom = nom;
    }

    public String getTelefon() {
        return telefon;
    }

    public void setTelefon(String telefon) {
        this.telefon = telefon;
    }

    public String getCorreu() {
        return correu;
    }

    public void setCorreu(String correu) {
        this.correu = correu;
    }
    
   
    public void updateComponent() {
        System.out.println("\nNIF de la persona: " + nif);
        System.out.println("\nEntra el nou nif:");
        nif = DADES.next();
        DADES.nextLine(); //Neteja buffer
        System.out.println("\nNom de la persona: " + nom);
        System.out.println("\nEntra el nou nom:");
        nom = DADES.nextLine();
        System.out.println("\nTelèfon de la persona: " + telefon);
        System.out.println("\nEntra el nou telèfon:");
        telefon = DADES.next();
        System.out.println("\nCorreu electrònic de la persona: " + correu);
        System.out.println("\nEntra el nou correu electrònic:");
        correu = DADES.next();
    }
    
    public void showComponent() {
        System.out.println("\nLes dades de la persona amb NIF " + nif + " són:");
        System.out.println("\nNom: " + nom);
        System.out.println("\nTelèfon: " + telefon);
        System.out.println("\nCorreu electrònic: " + correu);
    }
    
}
