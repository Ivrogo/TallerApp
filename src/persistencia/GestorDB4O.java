package persistencia;


import com.db4o.Db4oEmbedded;
import com.db4o.ObjectContainer;
import com.db4o.config.EmbeddedConfiguration;
import com.db4o.query.Predicate;
import java.util.List;
import model.Taller;
import principal.GestorTallerMecanicException;

/**
 *
 * @author fta
 */
public class GestorDB4O implements ProveedorPersistencia {

    private ObjectContainer db;
    private Taller taller;

    public Taller getTaller() {
        return taller;
    }

    public void setTaller(Taller taller) {
        this.taller = taller;
    }

    /*
     *TODO
     * 
     *Paràmetres: cap
     *
     *Acció:
     *  - Heu de crear / obrir la base de dades "EAC112122S1.db4o"
     *  - Aquesta base de dades ha de permetre que Taller s'actualitzi en cascada.
     *
     *Retorn: cap
     *
     */
    public void estableixConnexio() {
        EmbeddedConfiguration config = Db4oEmbedded.newConfiguration();
        config.common().objectClass(Taller.class).cascadeOnUpdate(true);
        db = Db4oEmbedded.openFile(config, "EAC112021S1.db4o");
    }

    public void tancaConnexio() {
        db.close();
    }

    /*
     *TODO
     * 
     *Paràmetres: el nom del fitxer i el taller a desar
     *
     *Acció:
     *  - Heu d'establir la connexio i al final tancar-la.
     *  - Heu de desar l'objecte Taller passat per paràmetre sobre la base de dades 
     *    (heu d'inserir-la si no existia ja a la base de dades, o actualitzar-la en l'altre cas)
     *  - S'ha de fer la consulta de l'existència amb Predicate
     *
     *Retorn: cap
     *
     */
    @Override
    public void desarTaller(String nomFitxer, Taller pTaller) throws GestorTallerMecanicException {
        
        estableixConnexio();
        final String cif = nomFitxer;
        
        List<Taller> tallers = db.query(new Predicate<Taller>() {
            public boolean match(Taller taller) {
                return (taller.getCif().equals(cif));
            }
        });
        
        if (tallers.size() != 1) {//No existeix
            db.store(pTaller);
            db.commit();
        } else {//Existeix
            taller = tallers.iterator().next();
            taller.setCif(pTaller.getCif());
            taller.setNom(pTaller.getNom());
            taller.setAdreca(pTaller.getAdreca());
            db.store(taller);
            db.commit();
        }
        tancaConnexio();
    }

    /*
     *TODO
     * 
     *Paràmetres: el nom del fitxer on està guardatel taller
     *
     *Acció:
     *  - Heu d'establir la connexio i al final tancar-la.
     *  - Heu de carregar el taller des de la base de dades assignant-la a l'atribut taller.
     *    Si no existeix, llanceu l'excepció GestorTallerMecanicException amb codi "GestorDB4O.noExisteix"
     *  - S'ha de fer la consulta amb Predicate
     *
     *Retorn: cap
     *
     */
    @Override
    public void carregarTaller(String nomFitxer) throws GestorTallerMecanicException {
        
        estableixConnexio();
        final String cif = nomFitxer;
        
        List<Taller> tallers = db.query(new Predicate<Taller>() {
            public boolean match(Taller taller) {
                return taller.getCif().equals(cif);
            }
        });
        
        if (tallers.size() != 1) {
            throw new GestorTallerMecanicException("GestorDB4O.noExisteix");
     
        } else {
            taller = tallers.iterator().next();
        }
        tancaConnexio();
    }
}
