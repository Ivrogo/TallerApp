package controlador;

import java.awt.event.ActionEvent;
import java.awt.event.ActionListener;
import javax.swing.JButton;
import javax.swing.JOptionPane;
import model.Recanvi;
import vista.MenuRecanvis;
import vista.RecanviForm;
import vista.RecanviLlista;

/**
 *
 * @author fta
 */
public class ControladorRecanvis implements ActionListener {

    private MenuRecanvis menuRecanvis;
    private RecanviForm recanviForm = null;
    private RecanviLlista recanviLlista = null;
    private int opcioSelec = 0;

    public ControladorRecanvis() {

        menuRecanvis = new MenuRecanvis();
        afegirListenersMenu();

    }

    private void afegirListenersMenu() {

        for (JButton boto : menuRecanvis.getMenuButtons()) {
            boto.addActionListener(this);
        }

    }

    private void afegirListenersForm() {

        recanviForm.getDesar().addActionListener(this);
        recanviForm.getSortir().addActionListener(this);

    }

    private void afegirListenersLlista() {

        recanviLlista.getSortir().addActionListener(this);

    }

    @Override
    public void actionPerformed(ActionEvent e) {

        //Accions per al menú
        JButton[] botons = menuRecanvis.getMenuButtons();

        for (int i = 0; i < botons.length; i++) {
            if (e.getSource() == botons[i]) {
                menuRecanvis.getFrame().setVisible(false);
                opcioSelec = i;
                seleccionarOpcio(i);
            }
        }

        //Accions per al formulari de mestres
        if (recanviForm != null) {

            if (e.getSource() == recanviForm.getDesar()) {

                if (opcioSelec == 1) {//Nou recanvi

                    ControladorPrincipal.getTallerActual().getComponents().add(new Recanvi(recanviForm.gettCodi().getText(), recanviForm.gettNom().getText(), recanviForm.gettFabricant().getText(), Double.parseDouble(recanviForm.gettPreu().getText())));

                }

            } else if (e.getSource() == recanviForm.getSortir()) { //Sortir

                recanviForm.getFrame().setVisible(false);
                menuRecanvis.getFrame().setVisible(true);

            }

        }

        if (recanviLlista != null) {

            if (e.getSource() == recanviLlista.getSortir()) {

                recanviLlista.getFrame().setVisible(false);
                menuRecanvis.getFrame().setVisible(true);

            }

        }

    }

    private void seleccionarOpcio(Integer opcio) {

        switch (opcio) {
            case 0: //sortir
                ControladorPrincipal.getMenuPrincipal().getFrame().setVisible(true);
                break;
            case 1: // alta
                if (ControladorPrincipal.getTallers()[0] != null) {
                    recanviForm = new RecanviForm();
                    afegirListenersForm();
                } else {
                    menuRecanvis.getFrame().setVisible(true);
                    JOptionPane.showMessageDialog(menuRecanvis.getFrame(), "Abans s'ha de crear al menys un taller en el menú de tallers.");
                }
                break;
            case 2: // llista
                if (ControladorPrincipal.getTallers()[0] != null) {
                    recanviLlista = new RecanviLlista();
                    afegirListenersLlista();
                } else {
                    menuRecanvis.getFrame().setVisible(true);
                    JOptionPane.showMessageDialog(menuRecanvis.getFrame(), "Abans s'ha de crear al menys un taller en el menú de tallers.");
                }

                break;
        }

    }

}
