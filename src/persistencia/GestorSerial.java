package persistencia;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import javax.swing.JOptionPane;
import model.Taller;
import principal.GestorTallerMecanicException;

/**
 *
 * @author fta
 */
public class GestorSerial implements ProveedorPersistencia{
    
    private Taller taller;

    public Taller getTaller() {
        return taller;
    }

    public void setTaller(Taller taller) {
        this.taller = taller;
    }    

    @Override
    public void desarTaller(String nomFitxer, Taller taller) throws GestorTallerMecanicException {
        /*
         *TODO
         *
         *Paràmetres: nom del fitxer i taller
         *
         *Acció:
         * - Ha de desar l'objecte Taller serialitzat sobre un fitxer del sistema 
         *   operatiu amb nom nomFitxer i extensió ".ser".
         * - Heu de controlar excepcions d'entrada/sortida i en cas de produïrse alguna, 
         *   llavors llançar GestorTallerMecanicException amb codi GestorSerial.desar 
         *
         *Nota: podeu comprovar que la classe Taller implementa Serializable  
         *
         *Retorn: cap
         */
        
        try(ObjectOutputStream oos =new ObjectOutputStream(new FileOutputStream(new File(nomFitxer + ".ser")))) {
            oos.writeObject(taller);
        } catch (IOException ex) {
            throw new GestorTallerMecanicException("GestorSerial.desar");
        }

    }

    @Override
    public void carregarTaller(String nomFitxer) throws GestorTallerMecanicException {
        /*
         *TODO
         *
         *Paràmetres: nom del fitxer
         *
         *Acció:
         * - Ha de carregar el fitxer del sistema operatiu amb nom nomFitxer i extensió 
         *   ".ser" sobre un nou objecte Taller que es retornarà com a resultat.               
         * - Heu de controlar excepcions d'entrada/sortida i en cas de produïrse alguna, 
         *   llavors llançar GestorTallerMecanicException amb codi GestorSerial.carrega 
         *
         *Retorn: cap
         */
        
        try(ObjectInputStream ois = new ObjectInputStream(new FileInputStream(new File(nomFitxer + ".ser")))) {
            taller = (Taller) ois.readObject();
        } catch (IOException ex) {
            throw new GestorTallerMecanicException("GestorSerial.carregar");
        } catch (ClassNotFoundException ex) {
            JOptionPane.showMessageDialog(null, "Error de classe: " + ex.getMessage());
        }

    }
}