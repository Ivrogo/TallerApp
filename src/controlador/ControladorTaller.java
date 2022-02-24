package controlador;

import java.awt.event.ActionEvent;
import java.awt.event.ActionListener;
import javax.swing.JButton;
import javax.swing.JOptionPane;
import model.Taller;
import persistencia.GestorDB4O;
import persistencia.GestorJDBC;
import persistencia.GestorPersistencia;
import persistencia.GestorSerial;
import persistencia.GestorXML;
import principal.GestorTallerMecanicException;
import vista.TallerForm;
import vista.TallerLlista;
import vista.MenuTaller;

/**
 *
 * @author fta
 */
public class ControladorTaller implements ActionListener {

    private MenuTaller menuTaller;
    private TallerForm tallerForm = null;
    private TallerLlista tallerLlista = null;
    private int opcioSelec = 0;

    public ControladorTaller() {

        /*
        TODO
        
        S'inicialitza l'atribut menuTaller (això mostrarà el menú de tallers)
        Es crida a afegirListenersMenu
        
         */
        
        menuTaller = new MenuTaller();
        afegirListenersMenu();

    }

    //El controlador com a listener dels controls de les finestres que gestionen els tallers
    //S'AFEGEIX EL CONTROLADOR COM A LISTENER DELS BOTONS DEL MENU
    private void afegirListenersMenu() {
        /*
        TODO
        
        A cada botó del menú de tallers, s'afegeix aquest mateix objecte (ControladorTaller) com a listener
        
         */
        
        for (JButton boto : menuTaller.getMenuButtons()) {
            boto.addActionListener(this);
        }

    }

    //S'AFEGEIX EL CONTROLADOR COM A LISTENER DELS BOTONS DESAR i SORTIR DEL FORMULARI
    private void afegirListenersForm() {
        /*
        TODO
        
        A cada botó del formulari del taller, s'afegeix aquest mateix objecte (ControladorTaller) com a listener
        
         */
        
        tallerForm.getDesar().addActionListener(this);
        tallerForm.getSortir().addActionListener(this);

    }

    //S'AFEGEIX EL CONTROLADOR COM A LISTENER DEL BOTO SORTIR DE LA LLISTA
    private void afegirListenersLlista() {
        /*
        TODO
        
        Al botó de sortir de la llista de tallers, s'afegeix aquest mateix objecte (ControladorTaller) com a listener
        */
        
        tallerLlista.getSortir().addActionListener(this);
    }

    @Override
    public void actionPerformed(ActionEvent e) {
        /*
        TODO
        
        Nota:
            Com ControladorTaller és listener del menú de tallers, del formulari i de la llista, llavors en aquest mètode
            actionPerformed heu de controlar si l'usuari ha premut algun botó de qualsevol dels esmentats frames.
            Ull! En el cas del formulari i de la llista, com provenen del menú (els llança el menú de tallerss), heu de verificar
            primer que els objectes tallerForm o tallerLlista no són nulls, per tal de saber si podeu comparar-los amb
            alguns dels botons d'aquests frames.
        
        Accions per al menú:
            
            S'ha de cridar a seleccionarOpcio segons l'opció premuda. Penseu que l'opció es correspon amb
            la posició que el botó ocupa a l'array de botons de menuTallers
            També, heu d'actualitzar la propietat opcioSelec (amb l'opció que ha premut l'usuari)
        
        Accions per al formulari de taller:
            
            ---- DESAR ----
            Si el botó premut per l'usuari és el botó de desar del formulari de tallers, llavors:
                - Es crea un nou objecte Taller amb les dades del formulari.
                - S'afegeix el taller creat a l'array de tallers de la classe ControladorPrincipal i s'actualitza
                  l'atribut pTallers també de la classe ControladorPrincipal.
                - Aquest taller passa a ser el tallerActual de la classe ControladorPrincipal i es canvia l'atribut
                  opcioSelec a 2 (seleccionar taller)
            
            ---- SORTIR ----
            Si el botó premut per l'usuari és el botó de sortir del formulari de tallers, llavors:
                - Heu de tornar al menú detallers (i amagar el formulari)
        
        Accions per a la llista de tallers:
            
            ---- SORTIR ----
            Si el botó premut per l'usuari és el botó de sortir de la llista de tallers, llavors:
                - Heu de tornar al menú de tallers (i amagar la llista)
         
         */
        
         //Accions per al menú
        JButton[] botons = menuTaller.getMenuButtons();

        for (int i = 0; i < botons.length; i++) {
            if (e.getSource() == botons[i]) {
                menuTaller.getFrame().setVisible(false);
                opcioSelec = i;
                seleccionarOpcio(i);
            }
        }

        //Accions per al formulari de tallers
        if (tallerForm != null) {

            if (e.getSource() == tallerForm.getDesar()) {

                if (opcioSelec == 1) {//Alta taller

                    Taller taller = new Taller(tallerForm.gettCif().getText(),tallerForm.gettNom().getText(),tallerForm.gettAdreca().getText());
                    ControladorPrincipal.getTallers()[ControladorPrincipal.getpTallers()] = taller;
                    ControladorPrincipal.setpTallers();
                    ControladorPrincipal.setTallerActual(taller);
                    opcioSelec = 2;

                }

            } else if (e.getSource() == tallerForm.getSortir()) { //Sortir

               tallerForm.getFrame().setVisible(false);
                menuTaller.getFrame().setVisible(true);

            }

        }

        if (tallerLlista != null) {

            if (e.getSource() == tallerLlista.getSortir()) {

                tallerLlista.getFrame().setVisible(false);
                menuTaller.getFrame().setVisible(true);

            }

        }

    }

    private void seleccionarOpcio(Integer opcio) {

        switch (opcio) {

            case 0: //sortir
                ControladorPrincipal.getMenuPrincipal().getFrame().setVisible(true);
                break;

            case 1: // alta
                if (ControladorPrincipal.getpTallers()< ControladorPrincipal.getMAXTALLERS()) {
                    tallerForm = new TallerForm();
                    afegirListenersForm();
                } else {
                    menuTaller.getFrame().setVisible(true);
                    JOptionPane.showMessageDialog(menuTaller.getFrame(), "Màxim nombre de tallers assolit.");
                }
                break;

            case 2: //seleccionar
                menuTaller.getFrame().setVisible(true);
                if (ControladorPrincipal.getTallers()[0] != null) {
                    seleccionarTaller();
                } else {
                    JOptionPane.showMessageDialog(menuTaller.getFrame(), "Abans s'ha de crear al menys un taller");
                }
                break;

            case 3: // llistar
                if (ControladorPrincipal.getTallers()[0] != null) {
                    tallerLlista = new TallerLlista();
                    afegirListenersLlista();
                } else {
                    menuTaller.getFrame().setVisible(true);
                    JOptionPane.showMessageDialog(menuTaller.getFrame(), "Abans s'ha de crear al menys un taller");
                }
                break;

            case 4: //carregar
                
                menuTaller.getFrame().setVisible(true);
                
                int code = JOptionPane.showOptionDialog(null, "Selecciona un mètode", "Carregar taller", 0, JOptionPane.QUESTION_MESSAGE, null, ControladorPrincipal.getMETODESPERSISTENCIA(), "XML");
                
                if (code != JOptionPane.CLOSED_OPTION) {
                    
                    GestorPersistencia gestor = new GestorPersistencia();
                    
                    Taller taller;
                    
                    try {
                        
                        String codi = JOptionPane.showInputDialog("Quin és el codi del taller que vols carregar?");
                        
                        gestor.carregarTaller(ControladorPrincipal.getMETODESPERSISTENCIA()[code], codi);
                        
                        if(ControladorPrincipal.getMETODESPERSISTENCIA()[code].equals("XML")){
                            taller = ((GestorXML)gestor.getGestor()).getTaller();
                        }else if(ControladorPrincipal.getMETODESPERSISTENCIA()[code].equals("Serial")){
                            taller = ((GestorSerial)gestor.getGestor()).getTaller();
                        }else if(ControladorPrincipal.getMETODESPERSISTENCIA()[code].equals("JDBC")){
                            taller = ((GestorJDBC)gestor.getGestor()).getTaller();
                        }else{
                            taller = ((GestorDB4O)gestor.getGestor()).getTaller();
                        }
                        
                        int pos = comprovarTaller(taller.getCif());
                        
                        if (pos >= 0) {
                            
                            Object[] options = {"OK", "Cancel·lar"};                            
                            int i = JOptionPane.showOptionDialog(null, "Premeu OK per substituir-lo.", "El taller ja existent",
                                    JOptionPane.DEFAULT_OPTION, JOptionPane.WARNING_MESSAGE,
                                    null, options, options[0]);
                            
                            if (i == 0) {
                                ControladorPrincipal.getTallers()[pos] = taller;
                            }
                            
                        } else {
                            
                            ControladorPrincipal.getTallers()[ControladorPrincipal.getpTallers()] = taller;
                            ControladorPrincipal.setpTallers();
                            JOptionPane.showMessageDialog(menuTaller.getFrame(), "Taller afegit correctament");
                        
                        }

                    } catch (GestorTallerMecanicException e) {
                        JOptionPane.showMessageDialog(menuTaller.getFrame(), e.getMessage());
                    }
                }
                
                break;

            case 5: //desar
                /*
                TODO
                
                Es comprova si s'ha seleccionat el taller, mostrant, si correspon, un missatges d'error (JOptionPane.showMessageDialog)
                Si s'ha seleccionat el taller:
                    - Es mostra un dialog (JOptionPane.showOptionDialog) amb botons, on cadascun d'ells és un mètode de càrrega
                      (atribut de Controlador Principal: ara XML i Serial)
                    - Un cop escollit el mètode, es desa el taller cridant a desarTaller del gestor de persistència.
                 */
                
                menuTaller.getFrame().setVisible(true);
                
                if (ControladorPrincipal.getTallerActual() != null) {
                    
                    int tipusMissatge = JOptionPane.QUESTION_MESSAGE;
                    int codi = JOptionPane.showOptionDialog(null, "Selecciona un mètode", "Desar taller", 0, tipusMissatge, null, ControladorPrincipal.getMETODESPERSISTENCIA(), "XML");
                    
                    if (codi != JOptionPane.CLOSED_OPTION) {
                        
                        GestorPersistencia gestor = new GestorPersistencia();
                        
                        try {                            
                            gestor.desarTaller(ControladorPrincipal.getMETODESPERSISTENCIA()[codi], ControladorPrincipal.getTallerActual().getCif(), ControladorPrincipal.getTallerActual());
                        } catch (GestorTallerMecanicException e) {                            
                            JOptionPane.showMessageDialog(menuTaller.getFrame(), e.getMessage());                            
                        }
                        
                    }
                    
                } else {                    
                    JOptionPane.showMessageDialog(menuTaller.getFrame(), "Abans s'ha de seleccionar un taller");                
                }

                break;

        }

    }

    private void seleccionarTaller() {

        String[] cifs = new String[ControladorPrincipal.getpTallers()];

        int i = 0;

        for (Taller taller : ControladorPrincipal.getTallers()) {

            if (taller != null) {
                cifs[i] = taller.getCif();
            }

            i++;

        }

        int messageType = JOptionPane.QUESTION_MESSAGE;
        int codi = JOptionPane.showOptionDialog(null, "Selecciona un taller", "Selecció del taller", 0, messageType, null, cifs, "A");
        
        if (codi != JOptionPane.CLOSED_OPTION) {
            ControladorPrincipal.setTallerActual(ControladorPrincipal.getTallers()[codi]);
        }

    }

    private int comprovarTaller(String cif) {

        boolean trobat = false;

        int pos = -1;

        for (int i = 0; i < ControladorPrincipal.getTallers().length && !trobat; i++) {

            if (ControladorPrincipal.getTallers()[i] != null) {
                if (ControladorPrincipal.getTallers()[i].getCif().equals(cif)) {
                    pos = i;
                    trobat = true;
                }
            }
        }

        return pos;
    }

}