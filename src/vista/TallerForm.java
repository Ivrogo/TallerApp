package vista;

import java.awt.GridLayout;
import javax.swing.JButton;
import javax.swing.JFrame;
import javax.swing.JLabel;
import javax.swing.JTextField;

/**
 *
 * @author fta
 */
public class TallerForm {
    
    private JFrame frame;
    
    private final int AMPLADA = 300;
    private final int ALCADA = 200;

    private JLabel lCif;
    private JTextField tCif;
    private JLabel lNom;
    private JTextField tNom;
    private JLabel lAdreca;
    private JTextField tAdreca;

    private JButton desar;   
    private JButton sortir;   

    public TallerForm() {
        /*
        TODO
        
        Amb els atributs d'aquesta classe, heu de fer el següent (no afegiu cap listener a cap control):
            
            - Heu de crear l'objecte JFrame amb títol "Formulari Taller" i layout Grid d'una columna
        
            - Heu de crear els controls del formulari (labels i textFields).
        
            - Heu de crear els botons del formulari
        
            - Heu d'afegir-ho tot al frame
        
            - Heu de fer visible el frame amb l'amplada i alçada de les constants AMPLADA i ALCADA, i fer que la finestra es tanqui 
            quan l'usuari ho fa amb el control "X" de la finestra. Per fer tot això, heu de cridar al mètode showFinestra() d'aquesta
            classe.       
        */
        
        //Definició de la finestra del menú
        frame = new JFrame("Formulari Taller");
        frame.setLayout(new GridLayout(0, 1));

        //Creació dels controls del formulari
        lCif = new JLabel("CIF");
        tCif = new JTextField(10);
        lNom = new JLabel("Nom");
        tNom = new JTextField(20);
        lAdreca = new JLabel("Adreça");
        tAdreca = new JTextField(20);

        //Creació dels botons del formulari
        desar = new JButton("Desar");
        sortir = new JButton("Sortir");

        //Addició del tot el formulari a la finestra
        frame.add(lCif);
        frame.add(tCif);
        frame.add(lNom);
        frame.add(tNom);
        frame.add(lAdreca);
        frame.add(tAdreca);
        frame.add(desar);       
        frame.add(sortir);

        showFinestra();
    }
    
    public TallerForm(String cif, String nom, String adreca){
        this();
        tCif.setText(cif);
        tNom.setText(nom);
        tNom.setText(adreca);
    }
    
    private void showFinestra(){
        //Es mostra la finestra amb propietats per defecte
        frame.setSize(AMPLADA, ALCADA);
        frame.setDefaultCloseOperation(JFrame.EXIT_ON_CLOSE);
        frame.setVisible(true);
    }

    public JFrame getFrame() {
        return frame;
    }

    public void setFrame(JFrame frame) {
        this.frame = frame;
    }

    public JLabel getlCif() {
        return lCif;
    }

    public void setlCif(JLabel lCif) {
        this.lCif = lCif;
    }

    public JTextField gettCif() {
        return tCif;
    }

    public void settCif(JTextField tCif) {
        this.tCif = tCif;
    }

    public JLabel getlNom() {
        return lNom;
    }

    public void setlNom(JLabel lNom) {
        this.lNom = lNom;
    }

    public JTextField gettNom() {
        return tNom;
    }

    public void settNom(JTextField tNom) {
        this.tNom = tNom;
    }

    public JLabel getlAdreca() {
        return lAdreca;
    }

    public void setlAdreca(JLabel lAdreca) {
        this.lAdreca = lAdreca;
    }

    public JTextField gettAdreca() {
        return tAdreca;
    }

    public void settAdreca(JTextField tAdreca) {
        this.tAdreca = tAdreca;
    }   

    public JButton getDesar() {
        return desar;
    }

    public void setDesar(JButton desar) {
        this.desar = desar;
    }

    public JButton getSortir() {
        return sortir;
    }

    public void setSortir(JButton sortir) {
        this.sortir = sortir;
    } 
    
}