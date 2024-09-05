CREATE OR REPLACE PROCEDURE main.spr_redshift_itau_contadores(OUT exit_msg character varying(65535))
 LANGUAGE plpgsql
AS $$

DECLARE
    target_day integer := DATE_PART(day, CURRENT_DATE - INTERVAL '1 day');
    target_day_2c varchar(2) := LPAD(target_day, 2, 0);
    target_month integer := DATE_PART(month, CURRENT_DATE - INTERVAL '1 day');
    target_month_2c varchar(2) := LPAD(target_month, 2, '0');
    target_year integer := DATE_PART(year, CURRENT_DATE - INTERVAL '1 day');
    target_full_date date := (target_year || target_month_2c || target_day_2c)::date;

    folder_name varchar(100) := 's3://cntcar-dlk-dev-us-east-2-564512845791-raw/engineering/prditau/contadores_pedidos/';
    folder_tmp  varchar(100) := 's3://cntcar-dlk-dev-us-east-2-564512845791-raw/engineering/prditau/contadores_pedidos/tmp/'; --used for automation
    file_sequential_name varchar(8) := target_year || target_month_2c || target_day_2c;

    iamrole_string varchar(255) := 'arn:aws:iam::564512845791:role/sdlf-engineering-AWSRedshiftRole';

    output_filename varchar(255) :=     folder_name || file_sequential_name || '_BaseContadores';
    output_tmp      varchar(255) :=     folder_tmp  || file_sequential_name || '_tb_ti5_info_pedi_tag-'; -- used for automation

    select_string varchar(40) :=        'SELECT * FROM #Base';
    unload_string varchar(1000) :=      'UNLOAD ('           || QUOTE_LITERAL(select_string)        || ')' ||
                                            ' TO '              || QUOTE_LITERAL(output_filename)   ||
                                            'IAM_ROLE'          || QUOTE_LITERAL(iamrole_string)                ||
                                            'DELIMITER '        || QUOTE_LITERAL(',')                           ||
                                            'HEADER
                                            ALLOWOVERWRITE
                                            PARALLEL OFF
                                            MAXFILESIZE AS 80 MB
                                            EXTENSION '         || QUOTE_LITERAL('csv');
    unload_tmp varchar(1000) :=         'UNLOAD ('           || QUOTE_LITERAL(select_string)        || ')' ||
                                            ' TO '              || QUOTE_LITERAL(output_tmp)   ||
                                            'IAM_ROLE'          || QUOTE_LITERAL(iamrole_string)                ||
                                            'DELIMITER '        || QUOTE_LITERAL(',')                           ||
                                            'HEADER
                                            ALLOWOVERWRITE
                                            PARALLEL OFF
                                            MAXFILESIZE AS 80 MB
                                            EXTENSION '         || QUOTE_LITERAL('csv'); --used for automation

BEGIN

-- CRIAÇÃO TABELA DE PEDIDOS ITAÚ
DROP TABLE IF EXISTS #PedidosAprovadosItau;
    SELECT
        DISTINCT a.numero_pedido AS NumeroPedido
        ,a.codigo_campanha AS CodigoCampanha
        ,a.Data_Pedido::DATE AS DataPedido
        ,nfe.datarealentrega::DATE DataEntrega
        ,nfc.nfcid CodigoNFC
        ,a.ativacao_automatica AS AtivacaoAutomatica
        ,a.canal_aquisicao AS CanalAquisicao
        ,a.CPF
        ,CASE
            WHEN a.forma_de_pagamento_id = 4 THEN 'Crédito'
            WHEN a.forma_de_pagamento_id = 7 THEN 'Débito'
            ELSE 'Outros'
        END AS FormaDePagamento
        ,CASE
            WHEN a.adesao_Substituida_Id IS NOT NULL OR pv.substituicao IS true THEN 1
            ELSE 0
        END AS Substituicao
        ,CASE
            WHEN a.valor_em_centavos <> 0 OR a.valor_em_centavos IS NULL OR pv.valortotalpedido <> 0 THEN 0
            ELSE 1
        END AS Isento
    INTO
        #PedidosAprovadosItau
    FROM
        stage.main.tb_conectcar_dbo_pedidos_pedidovendaimportado as a
        LEFT JOIN stage.main.tb_conectcar_dbo_pedidovenda pv
            ON pv.numeropedido = a.numero_pedido
        LEFT JOIN stage.main.tb_controlloop_dbo_integracaopedido ip
            ON a.numero_pedido = ip.numeropedidoorigem
        LEFT JOIN stage.main.tb_controlloop_dbo_nfenviadatransportadora nfe 
            ON nfe.integracaopedidoid = ip.integracaopedidoid
        LEFT JOIN stage.main.tb_controlloop_dbo_nfenviadatransportadoranfc nfc
            ON nfc.nfenviada_transportadora_id = nfe.nfenviadatransportadoraid
    WHERE
        a.status_pedido IN (3,6)
        AND a.Codigo_Campanha ILIKE '%OfertaItauVitalicia%'
        AND a.Data_Pedido >= DATE_TRUNC('month', DATEADD(month, -6, target_full_date))
        AND a.Data_Pedido <  DATEADD(day, 1, target_full_date)
        AND ip.origempedidoid IN (17,18,32,33,34,35) -- filtrando canais itau
        ; 
    
    -- ADICIONANDO DATA DE ADESÃO NA TABELA DE PEDIDOS APROVADOS ITAÚ
    DROP TABLE IF EXISTS #AdesaoItau;
    SELECT
        DISTINCT p.NumeroPedido
        ,p.CodigoCampanha
        ,p.DataPedido AS DataPedido
        ,p.DataEntrega AS DataEntrega
        ,b.Data AS DataAdesao
        ,p.AtivacaoAutomatica
        ,p.CanalAquisicao
        ,p.CPF
        ,p.FormaDePagamento
        ,p.Substituicao
        ,CASE
            WHEN Substituicao = 0 THEN NULL
            ELSE p.Isento
        END AS Isento
    INTO
        #AdesaoItau
    FROM
        #PedidosAprovadosItau p
        LEFT JOIN
            (
                    SELECT
                        AdesaoId
                        , Data
                        , CodigoNFC
                    FROM
                        (
                            SELECT
                                a.AdesaoId
                                , a.Data::DATE
                                , a.CodigoNFC
                                , ROW_NUMBER() OVER (PARTITION BY a.TagId ORDER BY a.Data DESC) AS rn
                            FROM
                                refined.main.tb_redshift_ciclodevida_ativacao AS a
                            WHERE
                                a.Data < DATEADD(day, 1, target_full_date)
                                
                                
                        )
                    WHERE
                        rn = 1
            ) b
            ON UPPER(b.CodigoNFC) = UPPER(p.CodigoNFC);

    -- LIMPEZA DE DUPLICADOS
    DROP TABLE IF EXISTS #AdesaoItauLimpa;
    SELECT
        CAST(NumeroPedido AS BIGINT) num_pedi
        ,CAST(CodigoCampanha AS VARCHAR) nom_camp_clie
        ,CAST(DataPedido AS DATE) dat_pedi_rlzd
        ,CAST(DataEntrega AS DATE) dat_entg_tag
        ,CAST(DataAdesao AS DATE) dat_atvc_tag
        ,CAST(CanalAquisicao AS VARCHAR) nom_cana_aqui
        ,CAST(CPF AS VARCHAR) num_cpf
        ,CAST(FormaDePagamento AS VARCHAR) nom_form_pgto
        ,CAST(AtivacaoAutomatica AS BOOL) ind_atvc_auta_tag
        ,CASE
           WHEN Substituicao = 1 THEN true
           WHEN Substituicao = 0 THEN false
         END AS ind_subt_tag
        ,CASE
            WHEN Substituicao = 0 THEN false
            ELSE true
        END AS ind_isen_fret_tag
    INTO
        #AdesaoItauLimpa
    FROM
        (
            SELECT
                *
                , ROW_NUMBER() OVER (PARTITION BY NumeroPedido ORDER BY DataAdesao ASC) AS rn
            FROM
                #AdesaoItau
        )
    WHERE
        rn = 1;

    -- GERANDO A COLUNA DE CONTADOR
    DROP TABLE IF EXISTS #base;
    SELECT
        DISTINCT CAST(ROW_NUMBER() OVER(ORDER BY a.num_pedi ASC) AS BIGINT) AS num_reg_pedi_rlzd
        ,a.*
    INTO
        #base
    FROM
        #AdesaoItauLimpa a
    ORDER BY
        num_reg_pedi_rlzd DESC;

    -- EXECUTE, DROPs E INSERT
    EXECUTE unload_string;
    EXECUTE unload_tmp; --used for automation

    DROP TABLE IF EXISTS #PedidosAprovadosItau;
    DROP TABLE IF EXISTS #AdesaoItau;
    DROP TABLE IF EXISTS #AdesaoItauLimpa;
    DROP TABLE IF EXISTS #base;

    INSERT INTO refined.main.tb_itau_historico_envio VALUES (output_filename, 'Base Contadores', getdate());
    exit_msg = 'Arquivo criado no S3 com sucesso';

END
$$