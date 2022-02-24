package persistencia;

import principal.GestorTallerMecanicException;
import model.Taller;

/**
 *
 * @author fta
 */
public class GestorPersistencia {

    private ProveedorPersistencia gestor;

    public ProveedorPersistencia getGestor() {
        return gestor;
    }

    public void desarTaller(String tipusPersistencia, String nomFitxer, Taller taller) throws GestorTallerMecanicException {
        switch (tipusPersistencia) {

            case "XML":
                gestor = new GestorXML();
                break;
            case "Serial":
                gestor = new GestorSerial();
                break;
            case "JDBC":
                gestor = new GestorJDBC();
                break;
            default:
                gestor = new GestorDB4O();
                break;

        }

        gestor.desarTaller(nomFitxer, taller);
    }

    public void carregarTaller(String tipusPersistencia, String nomFitxer) throws GestorTallerMecanicException {

        switch (tipusPersistencia) {

            case "XML":
                gestor = new GestorXML();
                break;
            case "Serial":
                gestor = new GestorSerial();
                break;
            case "JDBC":
                gestor = new GestorJDBC();
                break;
            default:
                gestor = new GestorDB4O();
                break;

        }

        gestor.carregarTaller(nomFitxer);
    }
}
