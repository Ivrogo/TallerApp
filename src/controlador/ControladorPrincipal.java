package controlador;

import java.awt.event.ActionEvent;
import java.awt.event.ActionListener;
import javax.swing.JButton;
import model.Taller;
import persistencia.GestorPersistencia;
import vista.MenuPrincipal;

/**
 *
 * @author fta
 */
public class ControladorPrincipal implements ActionListener {

    static private MenuPrincipal menuPrincipal;
    static private final int MAXTALLERS = 4;
    static private Taller[] tallers = new Taller[MAXTALLERS];
    static private int pTallers = 0; //Priemra posició buida del vector tallers
    static private Taller tallerActual = null;
    static private int tipusComponent = 0;
    static private GestorPersistencia gp = new GestorPersistencia();
    static private final String[] METODESPERSISTENCIA = {"XML", "Serial", "JDBC", "DB4O"};

    public ControladorPrincipal() {
        /*
        TODO
        
        S'inicialitza l'atribut menuPrincipal (això mostrarà el menú principal)
        A cada botó del menú, s'afegeix aquest mateix objecte (ControladorPrincipal) com a listener
        
         */

        menuPrincipal = new MenuPrincipal();

        //S'AFEGEIX EL CONTROLADOR COM A LISTENER DELS BOTONS
        for (JButton boto : menuPrincipal.getMenuButtons()) {
            boto.addActionListener(this);
        }

    }

    @Override
    public void actionPerformed(ActionEvent e) {
        /*
        TODO

        S'ha de cridar a seleccionarOpcio segons l'opció premuda. Penseu que l'opció es 
        correspon amb la posició que el botó ocupa a l'array de botons de menuPrincipal.
        
         */

        JButton[] botons = menuPrincipal.getMenuButtons();
        for (int i = 0; i < botons.length; i++) {
            if (e.getSource() == botons[i]) {
                seleccionarOpcio(i);
            }
        }
    }

    private void seleccionarOpcio(int opcio) {

        switch (opcio) {
            case 0:
                System.exit(0);
                break;
            case 1:
                menuPrincipal.getFrame().setVisible(false);
                ControladorTaller controladorTaller = new ControladorTaller();
                break;
            case 2:
                menuPrincipal.getFrame().setVisible(false);
                ControladorRecanvis controladorRecanvis = new ControladorRecanvis();
                break;
        }

    }

    public static MenuPrincipal getMenuPrincipal() {
        return menuPrincipal;
    }

    public static void setMenuPrincipal(MenuPrincipal menuPrincipal) {
        ControladorPrincipal.menuPrincipal = menuPrincipal;
    }

    public static Taller[] getTallers() {
        return tallers;
    }

    public static void setTallers(Taller[] tallers) {
        ControladorPrincipal.tallers = tallers;
    }

    public static int getpTallers() {
        return pTallers;
    }

    public static void setpTallers() {
        ControladorPrincipal.pTallers++;
    }

    public static Taller getTallerActual() {
        return tallerActual;
    }

    public static void setTallerActual(Taller tallerActual) {
        ControladorPrincipal.tallerActual = tallerActual;
    }

    public static int getTipusComponent() {
        return tipusComponent;
    }

    public static void setTipusComponent(int tipusComponent) {
        ControladorPrincipal.tipusComponent = tipusComponent;
    }

    public static GestorPersistencia getGp() {
        return gp;
    }

    public static void setGp(GestorPersistencia gp) {
        ControladorPrincipal.gp = gp;
    }

    public static int getMAXTALLERS() {
        return MAXTALLERS;
    }

    public static String[] getMETODESPERSISTENCIA() {
        return METODESPERSISTENCIA;
    }

}
