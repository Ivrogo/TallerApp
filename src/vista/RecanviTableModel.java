package vista;

import controlador.ControladorPrincipal;
import javax.swing.table.AbstractTableModel;
import model.Recanvi;
import principal.Component;

/**
 *
 * @author fta
 */
public class RecanviTableModel extends AbstractTableModel {

    private final String[] columnNames = {"Codi", "Nom", "Fabricant", "Preu"};

    private String[][] data;

    public RecanviTableModel() {
        
        int i = 0;
        
        int totalRecanvis = 0;

        for (int j = 0; j < ControladorPrincipal.getTallerActual().getComponents().size(); j++) {

            if (ControladorPrincipal.getTallerActual().getComponents().get(j) instanceof Recanvi) {
                totalRecanvis++;
            }           
        }

        data = new String[totalRecanvis][4];
        
        for (Component component : ControladorPrincipal.getTallerActual().getComponents()) {
            if (component instanceof Recanvi) {
                data[i][0] = ((Recanvi)component).getCodi();
                data[i][1] = ((Recanvi)component).getNom();
                data[i][2] = ((Recanvi)component).getFabricant();
                data[i][3] = String.valueOf(((Recanvi)component).getPreu());
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
