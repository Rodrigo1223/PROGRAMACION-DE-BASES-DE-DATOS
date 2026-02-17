/* =============================================================================
   PROGRAMACIÓN DE BASES DE DATOS - ACTIVIDAD SEMANA 6
   PROYECTO: SISTEMA DE GESTIÓN AINTEGRAEDI
   -----------------------------------------------------------------------------
   DESCRIPCIÓN: Implementación de lógica para el cobro de multas y generación
   de reportes para departamentos con "Pago Cero".
   ============================================================================= */

-- 1. PROCEDIMIENTO: PRC_GRABA_DEUDORES_PAGO_CERO
-- Propósito: Modularizar la inserción en la tabla de reporte para limpieza del código.
CREATE OR REPLACE PROCEDURE PRC_GRABA_DEUDORES_PAGO_CERO(
    p_periodo   NUMBER,
    p_id_edif   NUMBER,
    p_nom_edif  VARCHAR2,
    p_run_adm   VARCHAR2,
    p_nom_adm   VARCHAR2,
    p_nro_depto NUMBER,
    p_run_resp  VARCHAR2,
    p_nom_resp  VARCHAR2,
    p_multa     NUMBER,
    p_obs       VARCHAR2
) IS
BEGIN
    /* Inserción directa en tabla de reporte. 
       Nota: Se usa el nombre de columna NOMBRE_ADMNISTRADOR según el modelo físico. */
    INSERT INTO GASTO_COMUN_PAGO_CERO (
        ANNO_MES_PCGC, 
        ID_EDIF, 
        NOMBRE_EDIF, 
        RUN_ADMINISTRADOR, 
        NOMBRE_ADMNISTRADOR,
        NRO_DEPTO, 
        RUN_RESPONSABLE_PAGO_GC,
        NOMBRE_RESPONSABLE_PAGO_GC,
        VALOR_MULTA_PAGO_CERO, 
        OBSERVACION
    ) VALUES (
        p_periodo, p_id_edif, p_nom_edif, p_run_adm, p_nom_adm,
        p_nro_depto, p_run_resp, p_nom_resp, p_multa, p_obs
    );
EXCEPTION
    WHEN OTHERS THEN
        RAISE_APPLICATION_ERROR(-20002, 'Error al insertar en reporte Pago Cero: ' || SQLERRM);
END;
/

-- 2. PROCEDIMIENTO PRINCIPAL: PRC_PROCESO_COBRO_AINTEGRAEDI
-- Propósito: Calcular multas, actualizar saldos y generar reporte de deudores.
CREATE OR REPLACE PROCEDURE PRC_PROCESO_COBRO_AINTEGRAEDI(
    p_periodo   NUMBER,
    p_valor_uf  NUMBER
) IS
    -- Manejo dinámico de periodos (Evita fechas fijas)
    v_peri_anterior NUMBER := TO_NUMBER(TO_CHAR(ADD_MONTHS(TO_DATE(p_periodo, 'YYYYMM'), -1), 'YYYYMM'));
    v_multa         NUMBER;
    v_obs           VARCHAR2(250);
    v_cant_deudas   NUMBER;
    v_fecha_corte   DATE;

    -- Cursor parametrizado para identificar deudores sin pagos en el mes anterior
    CURSOR c_deudores (p_periodo_busq NUMBER) IS
        SELECT 
            g.ID_EDIF, e.NOMBRE_EDIF, 
            (a.numrun_adm || '-' || a.dvrun_adm) as RUN_A,
            (a.pnombre_adm || ' ' || a.appaterno_adm) AS NOM_A,
            g.NRO_DEPTO, 
            (r.numrun_rpgc || '-' || r.dvrun_rpgc) as RUN_R,
            (r.pnombre_rpgc || ' ' || r.appaterno_rpgc) AS NOM_R
        FROM GASTO_COMUN g
        JOIN EDIFICIO e ON g.ID_EDIF = e.ID_EDIF
        JOIN ADMINISTRADOR a ON e.numrun_adm = a.numrun_adm
        JOIN RESPONSABLE_PAGO_GASTO_COMUN r ON g.numrun_rpgc = r.numrun_rpgc
        WHERE g.ANNO_MES_PCGC = p_periodo_busq
          AND NOT EXISTS (
              SELECT 1 FROM PAGO_GASTO_COMUN p 
              WHERE p.ID_EDIF = g.ID_EDIF 
                AND p.NRO_DEPTO = g.NRO_DEPTO 
                AND p.ANNO_MES_PCGC = g.ANNO_MES_PCGC
          )
        ORDER BY e.NOMBRE_EDIF ASC, g.NRO_DEPTO ASC;

    -- Excepción personalizada
    e_valor_invalido EXCEPTION;

BEGIN
    -- Validación de parámetro de entrada
    IF p_valor_uf <= 0 THEN RAISE e_valor_invalido; END IF;

    FOR reg IN c_deudores(v_peri_anterior) LOOP
        
        -- Cálculo de reincidencia: Contar meses sin pagos hasta el mes anterior
        SELECT COUNT(*) INTO v_cant_deudas
        FROM GASTO_COMUN gc
        WHERE gc.ID_EDIF = reg.ID_EDIF AND gc.NRO_DEPTO = reg.NRO_DEPTO
          AND gc.ANNO_MES_PCGC <= v_peri_anterior
          AND NOT EXISTS (
              SELECT 1 FROM PAGO_GASTO_COMUN p 
              WHERE p.ID_EDIF = gc.ID_EDIF AND p.NRO_DEPTO = gc.NRO_DEPTO AND p.ANNO_MES_PCGC = gc.ANNO_MES_PCGC
          );

        -- Lógica de negocio para multas según tramos
        IF v_cant_deudas = 1 THEN
            v_multa := 2 * p_valor_uf;
            v_obs := 'Se dará aviso de corte del combustible y agua.';
        ELSE
            v_multa := 4 * p_valor_uf;
            -- Obtención de fecha de corte según tabla GASTO_COMUN del periodo actual
            SELECT fecha_pago_gc INTO v_fecha_corte 
            FROM GASTO_COMUN 
            WHERE ID_EDIF = reg.ID_EDIF AND NRO_DEPTO = reg.NRO_DEPTO AND ANNO_MES_PCGC = p_periodo;
            
            v_obs := 'Se procederá al corte del combustible y agua el: ' || TO_CHAR(v_fecha_corte, 'DD/MM/YYYY');
        END IF;

        -- Actualización del gasto común actual
        UPDATE GASTO_COMUN 
        SET MULTA_GC = v_multa, 
            MONTO_TOTAL_GC = MONTO_TOTAL_GC + v_multa
        WHERE ID_EDIF = reg.ID_EDIF AND NRO_DEPTO = reg.NRO_DEPTO AND ANNO_MES_PCGC = p_periodo;

        -- Registro en tabla de reporte
        PRC_GRABA_DEUDORES_PAGO_CERO(
            p_periodo, reg.ID_EDIF, reg.NOMBRE_EDIF, reg.RUN_A, reg.NOM_A,
            reg.NRO_DEPTO, reg.RUN_R, reg.NOM_R, v_multa, v_obs
        );
        
    END LOOP;

    COMMIT;

EXCEPTION
    WHEN e_valor_invalido THEN
        DBMS_OUTPUT.PUT_LINE('Error: El valor de la UF debe ser mayor a cero.');
    WHEN OTHERS THEN
        ROLLBACK;
        DBMS_OUTPUT.PUT_LINE('Error crítico en el proceso: ' || SQLERRM);
END;
/

-- BLOQUE DE PRUEBA: Validación final con parámetros solicitados
DELETE FROM GASTO_COMUN_PAGO_CERO;
COMMIT;

BEGIN
    -- Se procesa periodo Mayo 2026 (ajustado a datos de origen) con UF a $29.509
    PRC_PROCESO_COBRO_AINTEGRAEDI(202605, 29509);
END;
/

-- Consulta de verificación de resultados
SELECT * FROM GASTO_COMUN_PAGO_CERO;