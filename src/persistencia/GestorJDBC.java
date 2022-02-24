package persistencia;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import model.Recanvi;
import model.Taller;
import principal.Component;
import principal.GestorTallerMecanicException;

/**
 *
 * @author fta
 */
public class GestorJDBC implements ProveedorPersistencia {

    private Taller taller;

    private Connection conn; //Connexió a la base de dades

    public Taller getTaller() {
        return taller;
    }

    public void setTaller(Taller taller) {
        this.taller = taller;
    }

    /*
     PreparedStatement necessaris
     */

 /*
     * TODO
     *
     * Obtenir un taller
     * 
     * Sentència select de la taula tallers
     * Columnes: totes
     * Files: la del taller el CIF del qual coincideixi amb el passat per paràmetre
     *
     */
    private static String codiTallerSQL = "SELECT * FROM tallers t WHERE t.cif = ?";

    private PreparedStatement codiTallerSt;

    /*
     * TODO
     *
     * Afegir un taller
     * 
     * Sentència per afegir un taller en la taula tallers
     * Els valors dels camps són els que es passaran per paràmetre
     *
     */
    private static String insereixTallerSQL = "INSERT INTO tallers(cif, nom, adreca) VALUES (?, ?, ?)";

    private PreparedStatement insereixTallerSt;

    /*
     * TODO
     *
     * Actualitzar un taller
     * 
     * Sentència per actualitzar un taller de la taula tallers
     * Files a actualitzar: la que corresponguin al CIF passat per paràmetre
     * Columnes a actualitzar: nom i adreca amb els altres valors passats per paràmetre
     *
     */
    private static String actualitzaTallerSQL = "UPDATE tallers SET nom = ?, adreca = ? WHERE cif = ?";

    private PreparedStatement actualitzaTallerSt;

    /*
     * TODO
     *
     * Eliminar recanvis (donat el codi d'un taller)
     * 
     * Sentència que elimina els recanvis de la taula recanvis relacionats amb un taller
     * Files a eliminar: les que es corresponguin al CIF del taller passat per paràmetre
     *
     */
    private static String eliminaRecanviSQL = "DELETE FROM recanvis WHERE taller = ?";

    private PreparedStatement eliminaRecanviSt;

    /*
     * TODO
     *
     * Afegir un recanvi
     * 
     * Sentència que afegix un recanvi a la taula de recanvis
     * Els valors dels camps són els que es passaran per paràmetre
     *
     */
    private static String insereixRecanviSQL = "INSERT INTO recanvis(codi, nom, fabricant, preu, taller) VALUES (?,?,?,?,?)";

    private PreparedStatement insereixRecanviSt;

    /*
     * TODO
     *
     * Seleccionar els recanvis d'un taller
     * 
     * Sentència que selecciona els recanvis de la taula recanvis d'un taller determinat
     * Columnes: totes
     * Files: totes les que el CIF del taller coincideixi amb el passat per paràmetre
     *
     */
    private static String selRecanvisSQL = "SELECT * FROM recanvis WHERE taller = ?";

    private PreparedStatement selRecanvisSt;

    /*
     *TODO
     * 
     *Paràmetres: cap
     *
     *Acció:
     *  - Heu d'establir la connexio JDBC amb la base de dades EAC112122S1
     *  - Heu de crear els objectes PrepareStatement declarats com a atributs d'aquesta classe
     *    amb els respectius SQL declarats com a atributs just sobre cadascun d'ells.
     *  - Heu de fer el catch de les possibles excepcions (en aquest mètode no llançarem GestorTallerMecanicException,
     *    simplement, mostreu el missatge a consola de l'excepció capturada)
     *
     *Retorn: cap
     *
     */
    public void estableixConnexio() throws SQLException {
        String urlBaseDades = "jdbc:derby://localhost:1527/EAC112122S1";
        String usuari = "root";
        String contrasenya = "root123";

        try {
            conn = DriverManager.getConnection(urlBaseDades, usuari, contrasenya);

            codiTallerSt = conn.prepareStatement(codiTallerSQL);
            insereixTallerSt = conn.prepareStatement(insereixTallerSQL);
            actualitzaTallerSt = conn.prepareStatement(actualitzaTallerSQL);
            eliminaRecanviSt = conn.prepareStatement(eliminaRecanviSQL);
            insereixRecanviSt = conn.prepareStatement(insereixRecanviSQL);
            selRecanvisSt = conn.prepareStatement(selRecanvisSQL);

        } catch (SQLException e) {
            System.out.println(e.getMessage());
            conn = null;
            throw e;
        }
    }

    public void tancaConnexio() throws SQLException {
        try {
            conn.close();
        } finally {
            conn = null;
        }
    }

    /*
     *TODO
     * 
     *Paràmetres: el nom del fitxer i el taller a desar
     *
     *Acció:
     *  - Heu de desar el taller sobre la base de dades:
     *      - El taller s'ha de desar a la taula tallers (nomFitxer passat per paràmetre és el codi del taller)
     *      - Cada recanvi del taller, s'ha de desar com a registre de la taula recanvis.
     *      - Heu de tenir en compte que si el taller ja existeix, heu de fer el següent:
     *          - Actualitzar el registre taller ja existent
     *          - Eliminar tots els recanvis d'aquest taller de la taula tallers i després inserir els nous com si es tractes
     *            d'un nou taller.
     *  - Si al fer qualsevol operació es produeix una excepció, llavors heu de llançar l'excepció GestorTallerMecanicException amb codi "GestorJDBC.desar"
     *
     *Retorn: cap
     *
     */
    @Override
    public void desarTaller(String nomFitxer, Taller taller) throws GestorTallerMecanicException {
        try {
            if (conn == null) {
                estableixConnexio();
            }

            codiTallerSt.setString(1, taller.getCif());
            ResultSet regisTaller = codiTallerSt.executeQuery();

            //Si existeix el taller..
            if (regisTaller.next()) {

                actualitzaTallerSt.setString(1, taller.getCif());
                actualitzaTallerSt.setString(2, taller.getNom());
                actualitzaTallerSt.setString(3, taller.getAdreca());

                actualitzaTallerSt.executeUpdate();

                //Eliminem els recanvis
                eliminaRecanviSt.setString(1, taller.getCif());
                eliminaRecanviSt.executeUpdate();

            } else { //Si no existeix el taller...   

                insereixTallerSt.setString(1, taller.getCif());
                insereixTallerSt.setString(2, taller.getNom());
                insereixTallerSt.setString(3, taller.getAdreca());

                insereixTallerSt.executeUpdate();
            }

            //Inserim recanvis a taller
            for (Component component : taller.getComponents()) {
                if (component != null && component instanceof Recanvi) {
                    insereixRecanviSt.setString(1, ((Recanvi) component).getCodi());
                    insereixRecanviSt.setString(2, ((Recanvi) component).getNom());
                    insereixRecanviSt.setString(3, ((Recanvi) component).getFabricant());
                    insereixRecanviSt.setDouble(4, ((Recanvi) component).getPreu());
                    insereixRecanviSt.setString(5, taller.getCif());

                    insereixRecanviSt.executeUpdate();
                }
            }

            tancaConnexio();

        } catch (SQLException e) {
            throw new GestorTallerMecanicException("GestorJDBC.desar");
        }
    }

    /*
     *TODO
     * 
     *Paràmetres: el nom del fitxer del taller
     *
     *Acció:
     *  - Heu de carregar el taller des de la base de dades (nomFitxer passat per paràmetre és el codi del taller)
     *  - Per fer això, heu de cercar el registre taller de la taula amb CIF = nomFitxer
     *  - A més, heu d'afegir els recanvis al taller a partir de la taula recanvis
     *  - Si al fer qualsevol operació es dona una excepció, llavors heu de llançar l'excepció GestorTallerMecanicException 
     *    amb codi "GestorJDBC.carregar"
     *  - Si el nomFitxer donat no existeix a la taula tallers (és a dir, el CIF = nomFitxer no existeix), llavors
     *    heu de llançar l'excepció GestorTallerMecanicException amb codi "GestorJDBC.noexist"
     *
     *Retorn: cap
     *
     */
    @Override
    public void carregarTaller(String nomFitxer) throws GestorTallerMecanicException {
        try {
            if (conn == null) {
                estableixConnexio();
            }

            codiTallerSt.setString(1, nomFitxer);
            ResultSet regisTaller = codiTallerSt.executeQuery();

            if (regisTaller.next()) {
                taller = new Taller(regisTaller.getString("cif"),
                        regisTaller.getString("nom"),
                        regisTaller.getString("adreca"));

                selRecanvisSt.setString(1, taller.getCif());

                ResultSet regisRecanvis = selRecanvisSt.executeQuery();

                while (regisRecanvis.next()) {
                    taller.addRecanvi(new Recanvi(regisRecanvis.getString("codi"),
                            regisRecanvis.getString("nom"),
                            regisRecanvis.getString("fabricant"),
                            regisRecanvis.getDouble("preu")));
                }
            } else {
                throw new GestorTallerMecanicException("GestorJDBC.noExisteix");
            }

            tancaConnexio();

        } catch (SQLException e) {
            throw new GestorTallerMecanicException("GestorJDBC.carregar");
        }
    }
}