/* *****************************************************************************
   SEMANA 3 - PROGRAMACIÓN DE BASES DE DATOS 
   Gestión de Morosidad y Asignación Médica - Clínica Ketekura
   
   - Ejecutar con conexión PRACT3_PRY2206 (Usuario PRY2206_P3).
   - Este script automatiza los requerimientos de acreditación y apoyo social.
   ***************************************************************************** */

SET SERVEROUTPUT ON;

--  AJUSTE DE ESTRUCTURA (Para evitar error de longitud en carga de datos)
ALTER TABLE CARGO MODIFY (NOMBRE VARCHAR2(50));

-- 1. DEFINICIÓN DE VARIABLE BIND PARA EL AÑO DE PROCESO (Paramétrico)
-- Según instrucciones: Si el proceso es en 2024, se evalúa el año 2023.
VARIABLE b_anno_proceso NUMBER;
EXEC :b_anno_proceso := EXTRACT(YEAR FROM SYSDATE); 

-- =============================================================================
-- CASO 1: SISTEMA DE ACREDITACIÓN - REPORTE DE PAGOS MOROSOS
-- =============================================================================
DECLARE
    -- Definición de VARRAY para almacenar las multas por día (Tabla 1)
    TYPE t_multa IS VARRAY(7) OF NUMBER;
    v_multas t_multa := t_multa(1200, 1300, 1700, 1900, 1100, 2000, 2300);
    
    -- Registro PL/SQL para estructurar la información del cursor
    TYPE t_reg_pago IS RECORD (
        v_run         PACIENTE.pac_run%TYPE,
        v_dv          PACIENTE.dv_run%TYPE,
        v_nombre      VARCHAR2(100),
        v_ate_id      ATENCION.ate_id%TYPE,
        v_f_venc      PAGO_ATENCION.fecha_venc_pago%TYPE,
        v_f_pago      PAGO_ATENCION.fecha_pago%TYPE,
        v_nom_esp     ESPECIALIDAD.nombre%TYPE,
        v_id_esp      ESPECIALIDAD.esp_id%TYPE,
        v_f_nac       PACIENTE.fecha_nacimiento%TYPE
    );
    
    v_item t_reg_pago;

    -- Cursor Explícito: Obtiene atenciones pagadas fuera de plazo en el año anterior
    CURSOR c_morosidad IS
        SELECT 
            p.pac_run, p.dv_run, 
            p.pnombre || ' ' || p.apaterno || ' ' || p.amaterno,
            pa.ate_id, pa.fecha_venc_pago, pa.fecha_pago, 
            e.nombre, e.esp_id, p.fecha_nacimiento
        FROM PACIENTE p
        JOIN ATENCION a ON p.pac_run = a.pac_run
        JOIN PAGO_ATENCION pa ON a.ate_id = pa.ate_id
        JOIN ESPECIALIDAD e ON a.esp_id = e.esp_id
        WHERE pa.fecha_pago > pa.fecha_venc_pago
          AND EXTRACT(YEAR FROM pa.fecha_venc_pago) = :b_anno_proceso - 1
        ORDER BY pa.fecha_venc_pago ASC, p.apaterno ASC;

    -- Variables de cálculo
    v_dias_atraso     NUMBER;
    v_monto_dia       NUMBER;
    v_porc_descto     NUMBER;
    v_multa_total     NUMBER;
    v_edad            NUMBER;

BEGIN
    -- Limpieza de tabla de resultados mediante SQL Dinámico
    EXECUTE IMMEDIATE 'TRUNCATE TABLE PAGO_MOROSO';

    OPEN c_morosidad;
    LOOP
        FETCH c_morosidad INTO v_item;
        EXIT WHEN c_morosidad%NOTFOUND;

        -- Cálculo de la diferencia de días entre pago y vencimiento
        v_dias_atraso := v_item.v_f_pago - v_item.v_f_venc;

        -- Asignación de multa según Especialidad (Tabla 1) usando el VARRAY
        IF v_item.v_id_esp IN (100, 500) THEN v_monto_dia := v_multas(1); -- Cirugía / Derma
        ELSIF v_item.v_id_esp IN (200, 300) THEN v_monto_dia := v_multas(2); -- Ortopedia / Traum.
        ELSIF v_item.v_id_esp IN (400, 600) THEN v_monto_dia := v_multas(3); -- Inmuno / Otorrino
        ELSIF v_item.v_id_esp IN (700, 800) THEN v_monto_dia := v_multas(4); -- Fisiatría / Med. Int.
        ELSIF v_item.v_id_esp = 900 THEN v_monto_dia := v_multas(5); -- Med. General
        ELSIF v_item.v_id_esp = 1000 THEN v_monto_dia := v_multas(6); -- Psiquiatría
        ELSE v_monto_dia := v_multas(7); -- Otros
        END IF;

        -- Lógica de descuento para Tercera Edad (Tabla PORC_DESCTO_3RA_EDAD)
        v_edad := TRUNC(MONTHS_BETWEEN(SYSDATE, v_item.v_f_nac) / 12);
        
        BEGIN
            SELECT (porcentaje_descto / 100) 
            INTO v_porc_descto 
            FROM PORC_DESCTO_3RA_EDAD 
            WHERE v_edad BETWEEN anno_ini AND anno_ter;
        EXCEPTION 
            WHEN NO_DATA_FOUND THEN v_porc_descto := 0; 
        END;

        -- Aplicación del beneficio y cálculo final
        v_multa_total := (v_dias_atraso * v_monto_dia) * (1 - v_porc_descto);

        -- Inserción de los datos procesados en la tabla destino
        INSERT INTO PAGO_MOROSO VALUES (
            v_item.v_run, v_item.v_dv, v_item.v_nombre, v_item.v_ate_id, 
            v_item.v_f_venc, v_item.v_f_pago, v_dias_atraso, v_item.v_nom_esp, v_multa_total
        );
    END LOOP;
    CLOSE c_morosidad;

    COMMIT;
    DBMS_OUTPUT.PUT_LINE('Caso 1: Proceso de Pagos Morosos finalizado con éxito.');
END;
/

-- =============================================================================
-- CASO 2: ASIGNACIÓN DE MÉDICOS AL SERVICIO PÚBLICO 
-- =============================================================================
DECLARE
    -- VARRAY para las destinaciones 
    TYPE t_destinos IS VARRAY(3) OF VARCHAR2(60);
    v_lugares t_destinos := t_destinos('Servicio de Atención Primaria de Urgencia (SAPU)', 
                                       'Hospitales del área de la Salud Pública', 
                                       'Centros de Salud Familiar (CESFAM)');

    -- Registro PL/SQL para manejar los datos del cursor (%TYPE para integridad)
    TYPE t_reg_medico IS RECORD (
        run           MEDICO.med_run%TYPE,
        dv            MEDICO.dv_run%TYPE,
        nombre_full   VARCHAR2(150),
        uni_nom       UNIDAD.nombre%TYPE,
        total_ate     NUMBER
    );
    v_med t_reg_medico;

    -- Cursor Explícito con el filtro de año paramétrico (:b_anno_proceso - 1)
    CURSOR c_medicos IS
        SELECT 
            m.med_run, 
            m.dv_run, 
            m.pnombre || ' ' || m.apaterno || ' ' || m.amaterno, 
            u.nombre, 
            COUNT(a.ate_id)
        FROM MEDICO m
        JOIN UNIDAD u ON m.uni_id = u.uni_id
        LEFT JOIN ATENCION a ON m.med_run = a.med_run 
             AND EXTRACT(YEAR FROM a.fecha_atencion) = :b_anno_proceso - 1
        GROUP BY m.med_run, m.dv_run, m.pnombre, m.apaterno, m.amaterno, u.nombre;

    v_correo_inst VARCHAR2(100);
    v_destinacion VARCHAR2(100);

BEGIN
    -- Limpieza de tabla dinámica
    EXECUTE IMMEDIATE 'TRUNCATE TABLE MEDICO_SERVICIO_COMUNIDAD';

    OPEN c_medicos;
    LOOP
        FETCH c_medicos INTO v_med;
        EXIT WHEN c_medicos%NOTFOUND;

        -- Lógica de Correo Institucional:
        -- 2 letras unidad + penúltima/antepenúltima apellido + 3 últimos RUN
        v_correo_inst := LOWER(SUBSTR(v_med.uni_nom, 1, 2) || 
                         SUBSTR(v_med.nombre_full, INSTR(v_med.nombre_full, ' ', -1) - 2, 2) || 
                         SUBSTR(v_med.run, -3)) || '@clinicak.cl';

        -- Lógica de Destinación basada en la carga de trabajo 
        IF v_med.uni_nom IN ('ATENCIÓN ADULTO', 'ATENCIÓN AMBULATORIA') THEN
            v_destinacion := v_lugares(1);
        ELSIF v_med.uni_nom = 'ATENCIÓN URGENCIA' THEN
            IF v_med.total_ate <= 3 THEN v_destinacion := v_lugares(1); 
            ELSE v_destinacion := v_lugares(2); END IF;
        ELSIF v_med.uni_nom = 'PSIQUIATRÍA Y SALUD MENTAL' THEN
            v_destinacion := v_lugares(3);
        ELSE
            v_destinacion := v_lugares(2);
        END IF;

        -- INSERT ESPECIFICANDO COLUMNAS 
        INSERT INTO MEDICO_SERVICIO_COMUNIDAD (
            unidad, 
            run_medico, 
            nombre_medico, 
            correo_institucional, 
            total_aten_medicas, 
            destinacion
        ) VALUES (
            v_med.uni_nom,
            v_med.run || '-' || v_med.dv,
            v_med.nombre_full,
            v_correo_inst,
            v_med.total_ate,
            v_destinacion
        );
    END LOOP;
    CLOSE c_medicos;

    COMMIT;
    DBMS_OUTPUT.PUT_LINE('Caso 2: Proceso de Médicos finalizado exitosamente.');
END;
/
-- CONSULTAS PARA REVISIÓN DE RESULTADOS
SELECT * FROM PAGO_MOROSO;
SELECT * FROM MEDICO_SERVICIO_COMUNIDAD;