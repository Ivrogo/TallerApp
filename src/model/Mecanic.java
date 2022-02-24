/*
* Classe que defineix un mecànic o mecànica que treballa al taller. Es defineixen pel seu NIF, nom, telèfon
* i el correu electrònic.
 */
package model;

/**
 *
 * @author fta
 */
public class Mecanic extends Persona {

    /*
     TODO CONSTRUCTOR
    
     Paràmetres: valors per tots els atributs de la classe.
    
     Accions:
     - Assignar als atributs els valors passats com a paràmetres.    
     */
    
    public Mecanic(String nif, String nom, String telefon, String correu) {
        super(nif, nom, telefon, correu);
    }

    /*
    TODO
    
     Paràmetres: cap
    
     Accions:
     - Demanar a l'usuari les dades per consola per crear un nou mecànic o mecànica. Les dades a demanar 
       són les que necessita el constructor.
     - També heu de tenir en compte que el nom pot ser una frase, per exemple, Josep Maria.
     
     Retorn: El nou mecànic creat o mecànica creada.
     */
    public static Mecanic addMecanic() {
        String nif, nom, telefon, correu;

        System.out.println("\nNIF del mecànic o mecànica:");
        nif = DADES.next();
        DADES.nextLine(); //Neteja buffer
        System.out.println("\nNom del mecànic o mecànica:");
        nom = DADES.nextLine();
        System.out.println("\nTelèfon del mecànic o mecànica:");
        telefon = DADES.next();        
        System.out.println("\nCorreu electrònic del mecànic o mecànica:");
        correu = DADES.next();

        return new Mecanic(nif, nom, telefon, correu);
    }
}
