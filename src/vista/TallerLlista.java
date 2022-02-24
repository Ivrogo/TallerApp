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
public final class TallerLlista {
    
    private JFrame frame;
    
    private final int AMPLADA = 600;
    private final int ALCADA = 200;
    
    private JTable tTallers;

    private JButton sortir;   
    

    public TallerLlista() {
        frame = new JFrame("Llista de Tallers");
        frame.setLayout(new GridLayout(0, 1));

        tTallers = new JTable(new TallerTableModel());
        
        sortir = new JButton("Sortir");

        frame.add(new JScrollPane(tTallers));  
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

    public JTable gettTallers() {
        return tTallers;
    }

    public void settTallers(JTable tTallers) {
        this.tTallers = tTallers;
    }

    public JButton getSortir() {
        return sortir;
    }

    public void setSortir(JButton sortir) {
        this.sortir = sortir;
    }
}
