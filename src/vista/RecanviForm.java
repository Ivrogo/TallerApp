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
public class RecanviForm {
    
    private JFrame frame;
    
    private final int AMPLADA = 300;
    private final int ALCADA = 200;

    private JLabel lCodi;
    private JTextField tCodi;
    private JLabel lNom;
    private JTextField tNom;
    private JLabel lFabricant;
    private JTextField tFabricant;
    private JLabel lPreu;
    private JTextField tPreu;

    private JButton desar;   
    private JButton sortir;   

    public RecanviForm() {

        frame = new JFrame("Formulari Taller");
        frame.setLayout(new GridLayout(0, 1));

        //Creació dels controls del formulari
        lCodi = new JLabel("Codi");
        tCodi = new JTextField(10);
        lNom = new JLabel("Nom");
        tNom = new JTextField(20);
        lFabricant = new JLabel("Fabricant");
        tFabricant = new JTextField(20);
        lPreu = new JLabel("Preu");
        tPreu = new JTextField(10);

        //Creació dels botons del formulari
        desar = new JButton("Desar");
        sortir = new JButton("Sortir");

        //Addició del tot el formulari a la finestra
        frame.add(lCodi);
        frame.add(tCodi);
        frame.add(lNom);
        frame.add(tNom);
        frame.add(lFabricant);
        frame.add(tFabricant);
        frame.add(lPreu);
        frame.add(tPreu);
        frame.add(desar);       
        frame.add(sortir);

        showFinestra();
    }
    
    public RecanviForm(String codi, String nom, String fabricant, double preu){
        this();
        tCodi.setText(codi);
        tNom.setText(nom);
        tFabricant.setText(fabricant);
        tPreu.setText(String.valueOf(preu));
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

    public JLabel getlCodi() {
        return lCodi;
    }

    public void setlCodi(JLabel lCodi) {
        this.lCodi = lCodi;
    }

    public JTextField gettCodi() {
        return tCodi;
    }

    public void settCodi(JTextField tCodi) {
        this.tCodi = tCodi;
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

    public JLabel getlFabricant() {
        return lFabricant;
    }

    public void setlFabricant(JLabel lFabricant) {
        this.lFabricant = lFabricant;
    }

    public JTextField gettFabricant() {
        return tFabricant;
    }

    public void settFabricant(JTextField tFabricant) {
        this.tFabricant = tFabricant;
    }

    public JLabel getlPreu() {
        return lPreu;
    }

    public void setlPreu(JLabel lPreu) {
        this.lPreu = lPreu;
    }

    public JTextField gettPreu() {
        return tPreu;
    }

    public void settPreu(JTextField tPreu) {
        this.tPreu = tPreu;
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