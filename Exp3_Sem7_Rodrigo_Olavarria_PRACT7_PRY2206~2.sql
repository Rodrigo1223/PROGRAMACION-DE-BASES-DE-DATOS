/* *********************************************************************************
   SISTEMA DE GESTIÓN CLÍNICA MAXSALUD - REPORTE DE MOROSIDAD
   Rodrigo Olavarria Carrasco
  ********************************************************************************* */

-- 1. Estructura para el manejo de multas diarias por especialidad
CREATE OR REPLACE TYPE tipo_multas AS VARRAY(10) OF NUMBER;
/

-- 2. Función para obtener la descripción de la especialidad
CREATE OR REPLACE FUNCTION FN_OBTENER_ESPECIALIDAD(p_esp_id NUMBER) 
RETURN VARCHAR2 IS
    v_nombre_esp ESPECIALIDAD.NOMBRE%TYPE;
BEGIN
    SELECT nombre INTO v_nombre_esp FROM ESPECIALIDAD WHERE esp_id = p_esp_id;
    RETURN v_nombre_esp;
EXCEPTION
    WHEN OTHERS THEN 
        RETURN 'No Definida';
END FN_OBTENER_ESPECIALIDAD;
/

-- 3. Package para centralizar variables de sesión y lógica de beneficios
CREATE OR REPLACE PACKAGE PKG_PAGOS_CLINICA IS
    -- Variables para persistir montos calculados durante la ejecución
    v_valor_multa      NUMBER := 0;
    v_valor_descto     NUMBER := 0;

    -- Función para el cálculo de rebaja por tramos de edad
    FUNCTION F_CALC_DESCTO_3RA_EDAD(p_edad NUMBER, p_monto_multa NUMBER) RETURN NUMBER;
END PKG_PAGOS_CLINICA;
/

CREATE OR REPLACE PACKAGE BODY PKG_PAGOS_CLINICA IS

    FUNCTION F_CALC_DESCTO_3RA_EDAD(p_edad NUMBER, p_monto_multa NUMBER) 
    RETURN NUMBER IS
        v_porc NUMBER := 0;
    BEGIN
        -- Se aplica descuento según la tabla de parámetros para mayores de 70 años
        IF p_edad > 70 THEN
            SELECT NVL(porcentaje_descto, 0) / 100 INTO v_porc
            FROM PORC_DESCTO_3RA_EDAD
            WHERE p_edad BETWEEN anno_ini AND anno_ter;
        END IF;
        
        RETURN ROUND(p_monto_multa * v_porc);
    EXCEPTION
        WHEN OTHERS THEN 
            RETURN 0;
    END F_CALC_DESCTO_3RA_EDAD;

END PKG_PAGOS_CLINICA;
/

-- 4. Procedimiento principal para el procesamiento de la morosidad
CREATE OR REPLACE PROCEDURE SP_GENERAR_PAGO_MOROSO IS
    -- Carga de valores de multas definidos por la clínica
    v_multas tipo_multas := tipo_multas(1200, 1300, 1700, 1900, 1100, 2000, 2300);
    
    v_dias_moro     NUMBER;
    v_multa_base    NUMBER;
    v_edad_atencion NUMBER;
    v_anno_proceso  NUMBER := EXTRACT(YEAR FROM SYSDATE) - 1;

    -- Cursor para identificar atenciones fuera de plazo el año anterior
    CURSOR c_atenciones IS
        SELECT p.pac_run, p.dv_run, 
               p.pnombre || ' ' || p.apaterno as nombre_pac, 
               a.ate_id, pa.fecha_venc_pago, pa.fecha_pago, 
               a.costo, p.fecha_nacimiento, a.fecha_atencion, m.esp_id
        FROM PACIENTE p
        JOIN ATENCION a ON p.pac_run = a.pac_run
        JOIN PAGO_ATENCION pa ON a.ate_id = pa.ate_id
        JOIN MEDICO m ON a.med_run = m.med_run
        WHERE pa.fecha_pago > pa.fecha_venc_pago
        AND EXTRACT(YEAR FROM pa.fecha_pago) = v_anno_proceso;
BEGIN
    -- Se reinicia la tabla de resultados para la nueva carga
    EXECUTE IMMEDIATE 'TRUNCATE TABLE PAGO_MOROSO';

    FOR r IN c_atenciones LOOP
        -- Determinación de días de atraso y edad a la fecha de atención
        v_dias_moro := r.fecha_pago - r.fecha_venc_pago;
        v_edad_atencion := FLOOR(MONTHS_BETWEEN(r.fecha_atencion, r.fecha_nacimiento) / 12);
        
        -- Selección de multa diaria basada en el ID de especialidad
        IF r.esp_id = 100 THEN v_multa_base := v_multas(1);
        ELSIF r.esp_id = 200 THEN v_multa_base := v_multas(2);
        ELSIF r.esp_id IN (300, 400) THEN v_multa_base := v_multas(3);
        ELSIF r.esp_id = 500 THEN v_multa_base := v_multas(4);
        ELSIF r.esp_id = 600 THEN v_multa_base := v_multas(5);
        ELSIF r.esp_id IN (700, 800) THEN v_multa_base := v_multas(6);
        ELSE v_multa_base := v_multas(7);
        END IF;

        -- Almacenamiento en variables del package y cálculo de beneficios
        PKG_PAGOS_CLINICA.v_valor_multa := v_multa_base * v_dias_moro;
        PKG_PAGOS_CLINICA.v_valor_descto := PKG_PAGOS_CLINICA.F_CALC_DESCTO_3RA_EDAD(v_edad_atencion, PKG_PAGOS_CLINICA.v_valor_multa);

        -- Inserción en la tabla PAGO_MOROSO con el detalle procesado
        INSERT INTO PAGO_MOROSO (pac_run, pac_dv_run, pac_nombre, ate_id, fecha_venc_pago, 
                                fecha_pago, dias_morosidad, especialidad_atencion, costo_atencion, 
                                monto_multa, observacion)
        VALUES (
            r.pac_run, r.dv_run, r.nombre_pac, r.ate_id, r.fecha_venc_pago, r.fecha_pago,
            v_dias_moro, FN_OBTENER_ESPECIALIDAD(r.esp_id), r.costo, 
            (PKG_PAGOS_CLINICA.v_valor_multa - PKG_PAGOS_CLINICA.v_valor_descto),
            CASE WHEN PKG_PAGOS_CLINICA.v_valor_descto > 0 THEN 'Con Descuento 3ra Edad' ELSE 'Sin Descuento' END
        );
    END LOOP;
    
    COMMIT;
END SP_GENERAR_PAGO_MOROSO;
/

-- 5. Trigger para asegurar que solo se registren morosidades reales
CREATE OR REPLACE TRIGGER TRG_AUDITA_MOROSIDAD
BEFORE INSERT ON PAGO_MOROSO
FOR EACH ROW
BEGIN
    IF :NEW.dias_morosidad <= 0 THEN
        RAISE_APPLICATION_ERROR(-20001, 'Error: Días de morosidad deben ser mayores a cero.');
    END IF;
END;
/

-- 6. Ejecución y visualización de resultados
EXEC SP_GENERAR_PAGO_MOROSO;

SELECT * FROM PAGO_MOROSO 
ORDER BY fecha_venc_pago ASC, pac_nombre ASC;

/* *********************************************************************************
   RESUMEN EJECUTIVO: RECAUDACIÓN POR CONCEPTO DE MOROSIDAD
   Este reporte consolida los montos totales procesados para la acreditación.
   ********************************************************************************* */

SELECT 
    especialidad_atencion AS "ESPECIALIDAD",
    COUNT(*) AS "CANTIDAD MOROSOS",
    TO_CHAR(SUM(costo_atencion), '$999G999G999') AS "TOTAL COSTO ATENCIONES",
    TO_CHAR(SUM(monto_multa), '$999G999G999') AS "TOTAL MULTAS POR COBRAR",
    -- Identificamos cuántos pacientes recibieron el beneficio de 3ra edad
    COUNT(CASE WHEN observacion = 'Con Descuento 3ra Edad' THEN 1 END) AS "BENEFICIOS APLICADOS"
FROM 
    PAGO_MOROSO
GROUP BY 
    especialidad_atencion
ORDER BY 
    SUM(monto_multa) DESC;