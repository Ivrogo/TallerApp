/*
 * Click nbfs://nbhost/SystemFileSystem/Templates/Licenses/license-default.txt to change this license
 * Click nbfs://nbhost/SystemFileSystem/Templates/Classes/Main.java to edit this template
 */
package controlador;

import java.awt.GridLayout;
import javax.swing.JButton;
import javax.swing.JFrame;
import javax.swing.JScrollPane;
    import javax.swing.JTable;
import javax.swing.table.TableColumn;
import vista.TallerTableModel;

/**
 *
 * @author fta
 */
public class Prova {

    public Prova() {

        String[] nomColumnes = {"Nom", "Cognom", "Professió", "Aturat"};
        Object[][] files = {
            {"Maria", "Llopis", "Pediatra", false},
            {"Joan", "Carracedo", "Lampista", true},
            {"Alicia", "Montero", "Enginyera", false}
        };

        JTable taula = new JTable(files, nomColumnes);

        TableColumn columna = taula.getColumnModel().getColumn(3);
        
        JFrame frame = new JFrame("Llista de Tallers");
        frame.setLayout(new GridLayout(0, 1));


        frame.add(new JScrollPane(taula));  

        frame.setDefaultCloseOperation(JFrame.EXIT_ON_CLOSE);
        frame.setVisible(true);

    }
    /**
     * @param args the command line arguments
     */
    public static void main(String[] args) {
        Prova prova = new Prova();
    }
    
}
