/* TRABAJO SEMANA 2: - TRUCK RENTAL */

-- Paso la fecha a una bind variable para que el proceso sea parametrico como pide la pauta
VARIABLE b_fec_proceso VARCHAR2(10);
EXEC :b_fec_proceso := TO_CHAR(SYSDATE, 'DD/MM/YYYY');

SET SERVEROUTPUT ON;

DECLARE
    -- Cursor para sacar a los empleados del rango 100-320 (SQL Documentado 1)
    CURSOR c_empleados IS
        SELECT e.id_emp, e.numrun_emp, e.dvrun_emp, e.pnombre_emp, e.appaterno_emp, 
               e.sueldo_base, e.fecha_nac, e.fecha_contrato, e.id_estado_civil,
               ec.nombre_estado_civil
        FROM empleado e
        JOIN estado_civil ec ON e.id_estado_civil = ec.id_estado_civil
        WHERE e.id_emp BETWEEN 100 AND 320;

    -- Uso variables %TYPE para que el script no falle si cambian los largos de la tabla
    v_id_emp        empleado.id_emp%TYPE;
    v_nombre        empleado.pnombre_emp%TYPE;
    v_sueldo        empleado.sueldo_base%TYPE;
    
    -- Variables para armar el usuario y la clave segun las reglas de negocio
    v_user          VARCHAR2(100);
    v_pass          VARCHAR2(100);
    v_antiguedad    NUMBER;
    v_letras_ape    VARCHAR2(2);
    v_cont_reg      NUMBER := 0; 
    v_total_meta    NUMBER := 23; -- Meta de empleados a procesar

BEGIN
    -- Limpio la tabla con SQL Dinamico para poder correr el bloque varias veces (SQL Documentado 2)
    EXECUTE IMMEDIATE 'TRUNCATE TABLE USUARIO_CLAVE'; 

    FOR r IN c_empleados LOOP
        
        -- Saco los datos y calculo la antiguedad aqui en el bloque para que sea mas eficiente (PL/SQL Documentado 1)
        v_id_emp     := r.id_emp;
        v_nombre     := r.pnombre_emp;
        v_sueldo     := ROUND(r.sueldo_base); -- Redondeo el sueldo segun la pauta
        v_antiguedad := TRUNC(MONTHS_BETWEEN(SYSDATE, r.fecha_contrato) / 12);

        -- Concateno los datos para el Nombre de Usuario
        v_user := LOWER(SUBSTR(r.nombre_estado_civil, 1, 1)) || 
                  LOWER(SUBSTR(v_nombre, 1, 3)) || 
                  LENGTH(v_nombre) || '*' || 
                  SUBSTR(v_sueldo, -1) || 
                  r.dvrun_emp || 
                  v_antiguedad;
        
        -- Si lleva menos de 10 años, le pego la X al final
        IF v_antiguedad < 10 THEN 
            v_user := v_user || 'X'; 
        END IF;

        -- Logica para las letras del apellido segun el estado civil (PL/SQL Documentado 2)
        IF r.id_estado_civil IN (10, 60) THEN -- Casado o Union Civil
            v_letras_ape := LOWER(SUBSTR(r.appaterno_emp, 1, 2));
        ELSIF r.id_estado_civil IN (20, 30) THEN -- Soltero o Divorciado
            v_letras_ape := LOWER(SUBSTR(r.appaterno_emp, 1, 1) || SUBSTR(r.appaterno_emp, -1));
        ELSIF r.id_estado_civil = 40 THEN -- Viudo
            v_letras_ape := LOWER(SUBSTR(r.appaterno_emp, -3, 2));
        ELSE -- Separado
            v_letras_ape := LOWER(SUBSTR(r.appaterno_emp, -2));
        END IF;

        -- Armo la clave final usando el mes y año de la base de datos
        v_pass := SUBSTR(r.numrun_emp, 3, 1) || 
                  (EXTRACT(YEAR FROM r.fecha_nac) + 2) || 
                  (SUBSTR(v_sueldo, -3) - 1) || 
                  v_letras_ape || 
                  v_id_emp || 
                  TO_CHAR(SYSDATE, 'MMYYYY');

        -- Inserto los registros generados en la tabla destino
        INSERT INTO USUARIO_CLAVE (id_emp, numrun_emp, dvrun_emp, nombre_empleado, nombre_usuario, clave_usuario)
        VALUES (v_id_emp, r.numrun_emp, r.dvrun_emp, v_nombre || ' ' || r.appaterno_emp, v_user, v_pass);

        v_cont_reg := v_cont_reg + 1;
    END LOOP;

    -- Si el contador llego a 23 guardo todo, si no, tiro rollback por si acaso
    IF v_cont_reg = v_total_meta THEN
        COMMIT;
        DBMS_OUTPUT.PUT_LINE('PROCESO FINALIZADO EXITOSAMENTE: ' || v_cont_reg || ' registros cargados.');
    ELSE
        ROLLBACK;
        DBMS_OUTPUT.PUT_LINE('ERROR: No se procesaron todos los registros.');
    END IF;
    
END;
/

-- Consulta final para ver los resultados ordenados por ID
SELECT * FROM USUARIO_CLAVE ORDER BY id_emp ASC;