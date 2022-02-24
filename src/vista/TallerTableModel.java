package vista;

import controlador.ControladorPrincipal;
import javax.swing.table.AbstractTableModel;
import model.Taller;

/**
 *
 * @author fta
 */
public class TallerTableModel extends AbstractTableModel{
    
    private final String[] columnNames = {"CIF", "Nom", "Adreça"};

    String[][] data = new String[ControladorPrincipal.getMAXTALLERS()][3];

    public TallerTableModel() {
        int i = 0;
        for (Taller taller : ControladorPrincipal.getTallers()) {
            if (taller != null) {
                data[i][0] = taller.getCif();
                data[i][1] = taller.getNom();
                data[i][2] = taller.getAdreca();
                i++;
            }
        }
    }

    @Override
    public int getRowCount() {
        return data.length;
    }

    @Override
    public int getColumnCount() {
        return data[0].length;
    }

    @Override
    public String getColumnName(int col) {
        return columnNames[col];
    }

    @Override
    public Object getValueAt(int row, int column) {
        return data[row][column];
    }
    
}
