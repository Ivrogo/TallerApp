/*
* Classe que defineix un client o clienta del taller. Es defineixen pel seu NIF, nom, telèfon
* i correu electrònic.
 */
package model;

/**
 *
 * @author fta
 */
public class Client extends Persona {

    /*
     TODO CONSTRUCTOR
    
     Paràmetres: valors per tots els atributs de la classe.
    
     Accions:
     - Assignar als atributs els valors passats com a paràmetres.    
     */
    public Client(String nif, String nom, String telefon, String correu) {
        super(nif, nom, telefon, correu);
    }
    
    /*
    TODO
    
     Paràmetres: cap
    
     Accions:
     - Demanar a l'usuari les dades per consola per crear un nou client o clienta. Les dades a demanar 
       són les que necessita el constructor.
     - També heu de tenir en compte que el nom pot ser una frase, per exemple, Maria Antonia.
     
     Retorn: El nou client creat o clienta creada.
     */
    public static Client addClient() {
        String nif, nom, telefon, correu;

        System.out.println("\nNIF del client o clienta:");
        nif = DADES.next();
        DADES.nextLine(); //Neteja buffer
        System.out.println("\nNom del client o clienta:");
        nom = DADES.nextLine();
        System.out.println("\nTelèfon del client o clienta:");
        telefon = DADES.next();
        System.out.println("\nCorreu electrònic del client o clienta:");
        correu = DADES.next();

        return new Client(nif, nom, telefon, correu);
    }
}
