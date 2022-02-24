package principal;

import model.Reparacio;
import model.Taller;
import model.Client;
import model.Mecanic;
import model.Recanvi;
import java.util.InputMismatchException;
import java.util.Scanner;
import persistencia.GestorPersistencia;
import persistencia.GestorXML;

/**
 *
 * @author fta
 */
public class Application {

    private final static Scanner DADES = new Scanner(System.in);

    private static Taller[] tallers = new Taller[10];
    private static int pTallers = 0; //Priemra posició buida del vector tallers
    private static Taller tallerActual = null;
    static private String FITXER = "taller";
    static private GestorPersistencia gp = new GestorPersistencia();

    public static void main(String[] args) {
        try {
            menuPrincipal();
        } catch (GestorTallerMecanicException e) {
            System.out.println("\n" + e.getMessage());
        }
    }

    private static void menuPrincipal() throws GestorTallerMecanicException{
        int opcio = 0;

        do {
            
            try {
                
            System.out.println("\nSelecciona una opció");
            System.out.println("\n0. Sortir");
            System.out.println("\n1. Gestió de tallers");
            System.out.println("\n2. Gestió de clients o clientes");
            System.out.println("\n3. Gestió de mecànics o mecàniques");
            System.out.println("\n4. Gestió de recanvis");
            System.out.println("\n5. Gestió de reparacions");
            opcio = DADES.nextInt();

            switch (opcio) {
                case 0:
                    break;
                case 1:
                    menuTallers();
                    break;
                case 2:
                    if (tallerActual != null) {
                        menuComponents(1);
                    } else {
                        System.out.println("\nPrimer s'ha de seleccionar el taller al menú Gestió de tallers");
                    }
                    break;
                case 3:
                    if (tallerActual != null) {
                        menuComponents(2);
                    } else {
                        System.out.println("\nPrimer s'ha de seleccionar el taller al menú Gestió de tallers");
                    }
                    break;
                case 4:
                    if (tallerActual != null) {
                        menuComponents(3);
                    } else {
                        System.out.println("\nPrimer s'ha de seleccionar el taller al menú Gestió de tallers");
                    }
                    break;
                case 5:
                    if (tallerActual != null) {
                        menuReparacions();
                    } else {
                        System.out.println("\nPrimer s'ha de seleccionar el taller al menú Gestió de tallers");
                    }
                    break;
                default:
                    System.out.println("\nS'ha de seleccionar una opció correcta del menú.");
                    break;
            }
            
            } catch (InputMismatchException e) {
                
                throw new GestorTallerMecanicException("1");
                
            } catch (ArrayIndexOutOfBoundsException e) {
                
                throw new GestorTallerMecanicException("9");
                
            }
            
        } while (opcio != 0);
    }

    public static void menuTallers() throws GestorTallerMecanicException, InputMismatchException, ArrayIndexOutOfBoundsException {
        int opcio = 0;

        do {
            int indexSel = -1;
            System.out.println("\nSelecciona una opció");
            System.out.println("\n0. Sortir");
            System.out.println("\n1. Alta");
            System.out.println("\n2. Seleccionar");
            System.out.println("\n3. Modificar");
            System.out.println("\n4. Llista de tallers");
            System.out.println("\n5. Carregar taller");
            System.out.println("\n6. Desar taller");
            opcio = DADES.nextInt();
            switch (opcio) {
                case 0:
                    break;
                case 1:
                    tallers[pTallers] = Taller.addTaller();
                    pTallers++;
                    break;
                case 2:
                    indexSel = selectTaller();
                    if (indexSel >= 0) {
                        tallerActual = tallers[indexSel];
                    } else {
                        System.out.println("\nNo existeix aquest taller");
                    }
                    break;
                case 3:
                    indexSel = selectTaller();
                    if (indexSel >= 0) {
                        tallers[indexSel].updateComponent();
                    } else {
                        System.out.println("\nNo existeix aquest taller");
                    }
                    break;
                case 4:
                    for (int i = 0; i < pTallers; i++) {
                        tallers[i].showComponent();
                    }
                    break;
                case 5: //Carregar taller
                    pTallers = 0;
                    tallers = new Taller[1]; //Per facilitar la feina, només podem carregar un taller
                    gp.carregarTaller("XML", FITXER);
                    tallers[pTallers] = ((GestorXML)gp.getGestor()).getTaller();
                    pTallers++;
                    break;
                case 6: //Desar taller
                    int pos = selectTaller();
                    if (pos >= 0) {
                        gp.desarTaller("XML", FITXER, tallers[pos]);
                    } else {
                        System.out.println("\nNo existeix aquest taller");
                    }
                    break;
                default:
                    System.out.println("\nS'ha de seleccionar una opció correcta del menú.");
                    break;
            }
        } while (opcio != 0);
    }
    
    public static void menuComponents(int tipus) throws GestorTallerMecanicException, InputMismatchException, ArrayIndexOutOfBoundsException{
        int opcio = 0;

        do {
            System.out.println("\nSelecciona una opció");
            System.out.println("\n0. Sortir");
            System.out.println("\n1. Alta");
            System.out.println("\n2. Modificar");
            System.out.println("\n3. Llistat");
            opcio = DADES.nextInt();
            switch (opcio) {
                case 0:
                    break;
                case 1:
                    switch (tipus) {
                        case 1:
                            tallerActual.addClient(null);
                            break;
                        case 2:
                            tallerActual.addMecanic(null);
                            break;
                        case 3:
                            tallerActual.addRecanvi(null);
                            break;
                    }
                    break;
                case 2:
                    int indexSel = tallerActual.selectComponent(tipus,null);
                    if (indexSel >= 0) {
                        tallerActual.getComponents().get(indexSel).updateComponent();
                    } else {
                        System.out.println("\nNo existeix aquest component");
                    }
                    break;
                case 3:
                    for (int i = 0; i < tallerActual.getComponents().size(); i++) {
                            if (tallerActual.getComponents().get(i) instanceof Client && tipus == 1) {
                                tallerActual.getComponents().get(i).showComponent();
                            } else if (tallerActual.getComponents().get(i) instanceof Mecanic && tipus == 2) {
                                tallerActual.getComponents().get(i).showComponent();
                            } else if (tallerActual.getComponents().get(i) instanceof Recanvi && tipus == 3) {
                                tallerActual.getComponents().get(i).showComponent();
                            }
                        }
                default:
                    System.out.println("\nS'ha de seleccionar una opció correcta del menú.");
                    break;
            }
        } while (opcio != 0);
    }

    /*
     TODO Heu de desenvolupar el menuReparacions amb les opcions que podeu veure.
     Nota: penseu que quan arribem aquí, l'atribut tallerActual no és null
       
     Opció 0. Sortir -->       Surt del menú i retorna al menú principal
     Opció 1. Alta -->         Crea una reparació del taller actual. Penseu que Taller sap crear reparacions        
     Opció 2. Modificar -->    Permet modificar una reparació que està donat d'alta al taller actual
     (per comprovar l'existència de la reparació tenim un mètode en la classe Taller que ens ajuda)     
     Opció 3. Assignar client o clienta -->   Assigna un client o clienta a una reparació, però penseu que Taller sap com fer-ho
     Opció 4. Assignar mecànic o mecànica -->   Assigna un mecànic o mecànica a una reparació, però penseu que Taller sap com fer-ho
     Opció 5. Assignar recanvi -->   Assigna un recanvi a una reparació però penseu que Taller sap com fer-ho
     Opció 6. Calcular preu -->   Calcula el preu d'una reparació però penseu que Taller sap com fer-ho
     Opció 7. Llista de reparacions --> Imprimeix les dades de les reparacions del taller actual
        
     A més, heu de fer una estructura iterativa per tornar a mostrar el menú sempre que no es premi l'opció 0 de sortida
     Recomanacions:
     - estructura de control switch-case per bifurcar les opcions
     - si no s'ha introduït cap opció de les de la llista, s'ha de mostrar el missatge
     "S'ha de seleccionar una opció correcta del menú."
     - definiu una variable opcio de tipus enter
     */
    public static void menuReparacions() throws GestorTallerMecanicException, InputMismatchException, ArrayIndexOutOfBoundsException{
        int opcio = 0;

        do {
            System.out.println("\nSelecciona una opció");
            System.out.println("\n0. Sortir");
            System.out.println("\n1. Alta");
            System.out.println("\n2. Modificar");
            System.out.println("\n3. Assignar client o clienta");
            System.out.println("\n4. Assignar mecànic o mecànica");
            System.out.println("\n5. Assignar recanvi");
            System.out.println("\n6. Calcular preu");
            System.out.println("\n7. Llista de reparacions");
            opcio = DADES.nextInt();
            switch (opcio) {
                case 0:
                    break;
                case 1:
                    tallerActual.addReparacio(null);
                    break;
                case 2:
                    int indexSel = tallerActual.selectComponent(4, null);
                    if (indexSel >= 0) {
                        tallerActual.getComponents().get(indexSel).updateComponent();
                    } else {
                        System.out.println("\nNo existeix aquesta reparació");
                    }
                    break;
                case 3:
                    tallerActual.addClientReparacio();
                    break;
                case 4:
                    tallerActual.addMecanicReparacio();
                    break;
                case 5:
                    tallerActual.addRecanviReparacio();
                    break;
                case 6:
                    tallerActual.calcularPreuReparacio();
                    break;
                case 7:
                    for (int i = 0; i < tallerActual.getComponents().size(); i++) {
                        if (tallerActual.getComponents().get(i) instanceof Reparacio) {
                            tallerActual.getComponents().get(i).showComponent();
                        }
                    }
                    break;
                default:
                    System.out.println("\nS'ha de seleccionar una opció correcta del menú.");
                    break;
            }
        } while (opcio != 0);
    }

    public static Integer selectTaller() {

        System.out.println("\nCIF del taller?:");
        String cif = DADES.next();

        for (int i = 0; i < pTallers; i++) {
            if (tallers[i].getCif().equals(cif)) {
                return i;
            }
        }
        return -1;
    }
}
