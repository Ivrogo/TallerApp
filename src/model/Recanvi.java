/*
* Classe que defineix un recanvi utilitzat en una reparació. Es defineixen pel seu codi, 
* nom, fabricant que el fabrica, preu i si ha estat assignat o no a una reparació.
*/
package model;
import principal.Component;

/**
 *
 * @author fta
 */
public class Recanvi implements Component{
        
    private String codi;
    private String nom;
    private String fabricant;
    private double preu;
    private boolean assignat;

    /*
     TODO CONSTRUCTOR
    
     Paràmetres: valors per tots els atributs de la classe menys assignat.
    
     Accions:
     - Assignar als atributs corresponents els valors passats com a paràmetres.
     - Assignar a l'atribut assignat el valor fals, ja que quan es crea un recanvi
       aquest no s'ha assignat a cap reparació.    
     */

    public Recanvi(String codi, String nom, String fabricant, double preu) {
        this.codi = codi;
        this.nom = nom;
        this.fabricant = fabricant;
        this.preu = preu;
        this.assignat = false;
    }
    
    //Nou constructor per crear els recanvis a partir d'un document XML
    public Recanvi(String codi, String nom, String fabricant, double preu, boolean assignat) {
        this.codi = codi;
        this.nom = nom;
        this.fabricant = fabricant;
        this.preu = preu;
        this.assignat = assignat;
    }

    /*
     TODO Heu d'implementar tots els mètodes accessors possibles.  
     */
    public String getCodi() {
        return codi;
    }

    public void setCodi(String codi) {
        this.codi = codi;
    }

    public String getNom() {
        return nom;
    }

    public void setNom(String nom) {
        this.nom = nom;
    }

    public String getFabricant() {
        return fabricant;
    }

    public void setFabricant(String fabricant) {
        this.fabricant = fabricant;
    }

    public double getPreu() {
        return preu;
    }

    public void setPreu(double preu) {
        this.preu = preu;
    }

    public boolean isAssignat() {
        return assignat;
    }

    public void setAssignat(boolean assignat) {
        this.assignat = assignat;
    }
    
    /*
    TODO
    
     Paràmetres: cap
    
     Accions:
     - Demanar a l'usuari les dades per consola per crear un nou recanvi. Les dades a demanar 
       són les que necessita el constructor.
     - També heu de tenir en compte que el nom o el fabricant poden ser una frase, per exemple, filtre d'oli
       o Würth components.
     
     Retorn: El nou recanvi creat.
     */
    public static Recanvi addRecanvi() {
        String codi, nom, fabricant;
        double preu;

        System.out.println("\nCodi del recanvi:");
        codi = DADES.next();
        DADES.nextLine(); //Neteja buffer
        System.out.println("\nNom del recanvi:");
        nom = DADES.nextLine();
        System.out.println("\nFabricant del recanvi:");
        fabricant = DADES.nextLine();        
        System.out.println("\nPreu del recanvi:");
        preu = DADES.nextDouble();

        return new Recanvi(codi, nom, fabricant, preu);
    }
    
    /*
     TODO
    
     Paràmetres: cap
    
     Accions:
     - Demanar a l'usuari que introdueixi les noves dades de l'objecte actual i
       modificar els atributs corresponents d'aquest objecte.
     - També heu de tenir en compte que el nom o el fabricant poden ser una frase, per exemple, filtre d'oli
       o Würth components.
     - Li heu de mostrar a l'usuari els valors dels atributs abans de modificar-los.
    
    NOTA: EL valor de l'atribut assignat no es pot modificar.
     
    Retorn: cap
     */
    public void updateComponent() {
        System.out.println("\nCodi del recanvi: " + codi);
        System.out.println("\nEntra el nou codi:");
        codi = DADES.next();
        DADES.nextLine(); //Neteja buffer
        System.out.println("\nNom del recanvi: " + nom);
        System.out.println("\nEntra el nou nom:");
        nom = DADES.nextLine();
        System.out.println("\nFabricant del recanvi: " + fabricant);
        System.out.println("\nEntra el nou fabricant:");
        fabricant = DADES.nextLine();
        System.out.println("\nPreu del recanvi: " + preu);
        System.out.println("\nEntra el nou preu:");
        preu = DADES.nextDouble();
    }
    
    public void showComponent() {
        System.out.println("\nLes dades del recanvi amb codi " + codi + " són:");
        System.out.println("\nNom: " + nom);
        System.out.println("\nFabricant: " + fabricant);
        System.out.println("\nPreu: " + preu);
        
        if(assignat){
          System.out.println("\nEl recnavi està assignat");  
        }else{
          System.out.println("\nEl recnavi no està assignat");   
        }
    }
}
