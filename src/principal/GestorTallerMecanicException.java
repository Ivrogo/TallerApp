package principal;

/**
 *
 * @author fta
 */
public class GestorTallerMecanicException extends Exception {

    private String codiCausa = "desconegut";
    private String missatge;

    public GestorTallerMecanicException(String codiCausa) {
        this.codiCausa = codiCausa;
        switch (codiCausa) {
            case "1":
                missatge = "L'opció introduïda no és numèrica";
                break;
            case "2":
                missatge = "El valor introduït no és correcte";
                break;
            case "3":
                missatge = "El mecànic o mecànica ja està assignat o assignada a la reparació";
                break;
            case "4":
                missatge = "El recanvi ja està assignat a la reparació";
                break;
            case "5":
                missatge = "El client o clienta ja existeix";
                break;
            case "6":
                missatge = "El mecànic o mecànica ja existeix";
                break;
            case "7":
                missatge = "El recanvi ja existeix";
                break;
            case "8":
                missatge = "La reparació ja existeix";
                break;
            case "9":
                missatge = "Ja no hi caben més components";
                break;
            case "GestorXML.model":
                missatge = "No s'ha pogut crear el model XML per desar el taller";
                break;
            case "GestorXML.desar":
            case "GestorSerial.desar":
            case "GestorJDBC.desar":
                missatge = "No s'ha pogut desar el taller a causa d'error d'entrada/sortida";
                break;
            case "GestorXML.carrega":
            case "GestorSerial.carrega":
            case "GestorJDBC.carregar":
                missatge = "No s'ha pogut carregar el taller a causa d'error d'entrada/sortida";
                break;
            case "GestorJDBC.noExisteix":
            case "GestorDB4O.noExisteix":
                this.missatge = "El fitxer no existeix";
                break;
            default:
                missatge = "Error desconegut";
                break;
        }
    }

    public String getMessage() {
        return "Amb el codi de causa:  " + codiCausa + ", s'ha generat el següent missatge: " + missatge;
    }
}
