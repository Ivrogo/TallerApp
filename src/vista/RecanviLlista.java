package vista;

import java.awt.GridLayout;
import javax.swing.JButton;
import javax.swing.JFrame;
import javax.swing.JScrollPane;
import javax.swing.JTable;

/**
 *
 * @author fta
 */
public final class RecanviLlista {
    
    private JFrame frame;
    
    private final int AMPLADA = 600;
    private final int ALCADA = 200;
    
    private JTable tRecanvis;

    private JButton sortir;   
    

    public RecanviLlista() {
        frame = new JFrame("Llista de Recanvis");
        frame.setLayout(new GridLayout(0, 1));

        tRecanvis = new JTable(new RecanviTableModel());
        
        sortir = new JButton("Sortir");

        frame.add(new JScrollPane(tRecanvis));  
        frame.add(sortir);

        showFinestra();
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

    public JTable gettRecanvis() {
        return tRecanvis;
    }

    public void settRecanvis(JTable tRecanvis) {
        this.tRecanvis = tRecanvis;
    }    

    public JButton getSortir() {
        return sortir;
    }

    public void setSortir(JButton sortir) {
        this.sortir = sortir;
    }
}
