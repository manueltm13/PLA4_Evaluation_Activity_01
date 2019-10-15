
import com.db.ItfDB;
import com.db.MysqlDB;


import java.util.ArrayList;
import java.util.Arrays;
import java.util.Scanner;

/**
 * CRUD
 * @author manuel
 */
public class CrudVentasPLA4 {
    private static ArrayList<String> tablas;

    /**
     * @param args the command line arguments
     */
    public static void main(String[] args) {
        ItfDB db = new MysqlDB();
        ArrayList<String> 
            menuTabla = db.getTables(), // Nombres de las tablas del esquema de la BDD
            menuAccion = new ArrayList<>(Arrays.asList("ALTA", "BAJA", "MODIFICACIÓN", "CONSULTA")),
            menuConsultas = new ArrayList<>(Arrays.asList("TODOS", "FILTRO"));
                 
        int optTabla = 0, optAccion = 0, optConsulta;
        
        do{
            // Al entrar por primera vez o al salir del menú de selección 
            // de acción mostraremos el menú de selección de tabla
            if(optAccion == 0)
                optTabla = menu(menuTabla, "TABLA");

            if(optTabla == 0)
                continue; // Si hemos seleccionado salir en el menú de selección
                            // de tabla saldremos del programa
           
            // Seleccionamos la acción a ejecutar en la BDD
            optAccion = menu(menuAccion, menuTabla.get(optTabla - 1).toUpperCase());
            // Ejecutamos la acción seleccionada y regresaremos a la selección de acción
            switch (optAccion){
                case 1: // ALTA
                    alta(menuTabla.get(optTabla - 1), db);
                    break;
                case 2: // BAJA
                    baja(menuTabla.get(optTabla - 1), db);
                    break;
                case 3: // MODIFICACIÓN
                    modificacion(menuTabla.get(optTabla - 1), db);
                    break;
                case 4: // CONSULTA
                    // Seleccionamos el tipo de consulta. Todos los registros o aplicamos un filtro
                    optConsulta = menu(menuConsultas, "CONSULTA " + menuTabla.get(optTabla - 1).toUpperCase());
                    if(optConsulta == 0)
                        break; // Si seleccionamos salir regresaremos al menú de selección de acción
                    // Ejecutamos la consulta y regresamos al menú de selección de acción
                    consulta(menuTabla.get(optTabla - 1), menuConsultas.get(optConsulta - 1), db);
            }
        }while(optTabla != 0);
        
    }
    
    /**
     * Alta de un registro en una tabla
     * @param table - Tabla donde insertar el registro
     * @param db - Inyección de dependencia con la BDD a tratar
     */
    private static void alta(String table, ItfDB db){
        String prompt = "(\tFecha: yyyy-m-d, valor nulo: @NULL@)\n\t-----------------------------------\n", val = "";
        ArrayList<String> values = new ArrayList<>();
        Scanner scnKeyboard = new Scanner(System.in);
        ArrayList<String[]> columns;
        
        // Obtengo los nombres y características principales de los atributos de la tabla
        columns = db.getColumns("tbl_" + table);
        if (columns == null){
            pause(db.getErrorMsg() + ".");
            return;
        }
        
        for(String[] field: columns){
            // En los atributos AUTOINCREMENT insertaremos un null
            if(field[1].contains("AUTOINCREMENT")){
                values.add(null);
                continue;
            }
            // Obtengo el valor para el atributo
            prompt += "\t" + field[0] + " (" + field[1] + "): ";
            do{
                cls();
                System.out.println("ALTA " + table.toUpperCase() + "\n");
                System.out.print(prompt);
                if(scnKeyboard.hasNextLine())
                    val = scnKeyboard.nextLine();
                // Si es de tipo NOTNULL no dejaré insertar un nulo
            } while (val.equals("@NULL@") && field[1].contains("NOTNULL"));
            if(val.equals("@NULL@")){
                values.add(null);
            }else
                values.add(val);
            prompt += val + "\n";
        }
        // Ejecuto el insert en la tabla
        if(db.insert("tbl_" + table, values) > 0)
            pause("Registro insertado correctamente.");
        else
            pause(db.getErrorMsg() + ".");
    }
    
    /**
     * Baja de un registro de la tabla
     * @param table - Tabla de donde eliminar el registro
     * @param db - Inyección de dependencia con la BDD a tratar 
     */
    private static void baja(String table, ItfDB db){
        String header = "", prompt = "", val = "", where = "";
        Scanner scnKeyboard = new Scanner(System.in);
        int cnt;
        ArrayList<String[]> columns;
        ArrayList<String> output;
        
        // Obtengo los nombres y características principales de los atributos de la tabla
        columns = db.getColumns("tbl_" + table);
        if (columns == null){
            pause(db.getErrorMsg() + ".");
            return;
        }

        // Busco los atributos que forman la primary key y pido sus valores
        for(String[] field: columns){
            if(!field[1].contains("PK"))
                continue;
            prompt += "\t" + field[0] + " (" + field[1].replace(", AUTOINCREMENT", "") + "): ";
            cls();
            System.out.println("BAJA " + table.toUpperCase() + "\n");
            System.out.print(prompt);
            if(scnKeyboard.hasNextLine())
                val = scnKeyboard.nextLine();
            prompt += val + "\n";
            // Mensaje para mostrar con el resultado de la operación
            if(header.equals(""))
                header += " CÓDIGO " + val;
            else
                header += ", " + val;
            // Forma la clausula where de la sentencia sql que eliminará el registro
            if(where.equals(""))
                where += field[0] + " = " + val;
            else
                where += " and " + field[0] + " = " +val;
        }

        // Compruebo si existe el registro ejecutando una sentencia select
        output = db.select("tbl_" + table, where);
        if (output == null){
            cls();
            pause(db.getErrorMsg() + ".");
            return;
        }else if(output.size() == 1){
            pause("El registro no existe");
            return;
        }
        
        // Si el registro existe lo elimino y muestro el resultado de la operación
        cls();
        System.out.println("BAJA " + table.toUpperCase() + header + "\n");
        if((cnt = db.delete("tbl_" + table, where)) > 0)
            pause("Registro eliminado correctamente.");
        else if(cnt == 0)
            pause("Código de registro inexistente.");
        else
            pause(db.getErrorMsg() + ".");
    }
    
    /**
     * Modificación de un registro de la tabla
     * @param table - Tabla donde modificar el registro
     * @param db - Inyección de dependencia con la BDD a tratar 
     */
    private static void modificacion(String table, ItfDB db){
        String prompt = "", val = "", header = "", values = "", where = "";
        Scanner scnKeyboard = new Scanner(System.in);
        ArrayList<String[]> columns;
        ArrayList<String> output;
        
        // Obtengo los nombres y características principales de los atributos de la tabla
        columns = db.getColumns("tbl_" + table);
        if (columns == null){
            pause(db.getErrorMsg() + ".");
            return;
        }

        // Busco los atributos que forman la primary key y pido sus valores
        for(String[] field: columns){
            if(!field[1].contains("PK"))
                continue;
            prompt += "\t" + field[0] + " (" + field[1].replace(", AUTOINCREMENT", "") + "): ";
            cls();
            System.out.println("MODIFICACIÓN " + table.toUpperCase() + "\n");
            System.out.print(prompt);
            if(scnKeyboard.hasNextLine())
                val = scnKeyboard.nextLine();
            prompt += val + "\n";
            // Mensaje para mostrar con el resultado de la operación
            if(header.equals(""))
                header += " CÓDIGO " + val;
            else
                header += ", " + val;
            // Forma la clausula where de la sentencia sql que eliminará el registro
            if(where.equals(""))
                where += field[0] + " = " + val;
            else
                where += " and " + field[0] + " = " +val;
        }

        // Compruebo si existe el registro ejecutando una sentencia select
        output = db.select("tbl_" + table, where);
        if (output == null){
            cls();
            pause(db.getErrorMsg() + ".");
            return;
        }else if(output.size() == 1){
            pause("El registro no existe");
            return;
        }

        // Si el registro existe pido los valores de los campos que no forman parte de la primary key
        prompt = "(\tFecha: yyyy-m-d, valor nulo: @NULL@)\n\t-----------------------------------\n";
        for(String[] field: columns){
            if(field[1].contains("PK"))
                continue;
            // Obtengo el valor para el atributo
            prompt += "\t" + field[0] + " (" + field[1] + "): ";
            do{
                cls();
                System.out.println("MODIFICACIÓN " + table.toUpperCase() + "\n");
                System.out.print(prompt);
                if(scnKeyboard.hasNextLine())
                    val = scnKeyboard.nextLine();
                // Si es de tipo NOTNULL no dejaré insertar un nulo
            } while (val.equals("@NULL@") && field[1].contains("NOTNULL"));
            prompt += val + "\n";
            if(val.equals("@NULL@"))
                val = field[0] + " = null";
            else
                val = field[0] + " = \"" + val.replace("\"", "\\\"").replace("'", "\\'") + "\"";
            if(values.equals(""))
                values += val;
            else
                values += ", " + val;
        }

        // Modifico el registro y muestro el resultado de la operación
        if(db.update("tbl_" + table, where, values) > 0)
            pause("Registro modificado correctamente.");
        else
            pause(db.getErrorMsg() + ".");
    }
    
    /**
     * Consulta de registros de la tabla
     * @param table - Tabla desde donde listar los registros
     * @param type - Tipo de consulta. "TODOS" o "FILTRO"
     * @param db - Inyección de dependencia con la BDD a tratar 
     */
    private static void consulta(String table, String type, ItfDB db){
        ArrayList<String> output;
        Scanner scnKeyboard = new Scanner(System.in);
        String where = "", header;

        // Si quiero listarlos todos monto el mensaje para el resultado
        // de la operación y la clausula where
        if(type.equals("TODOS")){
            where = "true";
            header = "TODOS";
        }else{
            // Si quiero aplicar un filtro solicito por teclado la clausula where
            cls();
            System.out.println("CONSULTA " + table.toUpperCase() + "\n");
            System.out.print("\nFiltro (clausula where): ");
            if(scnKeyboard.hasNextLine())
                where = scnKeyboard.nextLine();
            header = where.trim();
        }

        // Ejecuto la selección de registros
        output = db.select("tbl_" + table, where);
        if (output == null){
            cls();
            pause(db.getErrorMsg() + ".");
            return;
        }
        
        // Si no hay errores muestro el listado
        cls();
        System.out.println("CONSULTA " + table.toUpperCase() + " " + header + "\n");
        System.out.println();
        for(String line: output)
            System.out.println(line);
        if(output.size() > 1){
            pause("Registros listados: " + (output.size() - 1));
        }else
            pause("Ningún registro en la selección");
    }
    
    /**
     * Menú de selección. Devuelve un entero correspondiente a la opción solicitada o
     * cero si el usuario selecciona salir
     * @param items - Elementos del menú
     * @param header - Mensaje para la cabecera del menú
     * @return - Opción elegida
     */
    private static int menu(ArrayList<String> items, String header){
        Scanner scnKeyboard = new Scanner(System.in);
        int i, opt = -1;
        
        do{
            cls();
            System.out.println(header + "\n");
            for(i = 0; i < items.size();)
                System.out.println("\t\t" + (i + 1) + ".- " + items.get(i++).toUpperCase());
            System.out.print("\nIndique una opción (0 para salir): ");
            if(scnKeyboard.hasNextInt())
                opt = scnKeyboard.nextInt();
            else
                scnKeyboard.next();
        }while(opt < 0 || opt > i );
        return opt;
    }
    
    // Limpio la pantalla de la consola
    private static void cls(){
        for(int i = 0; i < 50; i++) System.out.println();
    }
    
    /**
     * Pausa hasta pulsar Enter con mensaje
     * @param msg - Mensaje a mostrar
     */
    private static void pause(String msg){
        Scanner scnKeyboard = new Scanner(System.in);

        System.out.println("\n" + msg);
        System.out.print("Pulse Enter para continuar...");
        scnKeyboard.nextLine();
    }
    
}
