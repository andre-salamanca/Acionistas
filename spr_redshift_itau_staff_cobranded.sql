CREATE OR REPLACE PROCEDURE main.spr_redshift_itau_staff_cobranded(OUT exit_msg character varying(65535))
 LANGUAGE plpgsql
AS $$

DECLARE
    target_day integer := DATE_PART(day, CURRENT_DATE - INTERVAL '1 day');
    target_day_2c varchar(2) := LPAD(target_day, 2, 0);
    target_month integer := DATE_PART(month, CURRENT_DATE - INTERVAL '1 day');
    target_month_2c varchar(2) := LPAD(target_month, 2, '0');
    target_year integer := DATE_PART(year, CURRENT_DATE - INTERVAL '1 day');
    target_full_date date := (target_year || target_month_2c || target_day_2c)::date;

    folder_name varchar(100) := 's3://cntcar-dlk-dev-us-east-2-564512845791-data-analytics/compartilhamento-areas-de-negocio/financas/staff itau/';
    file_sequential_name varchar(8) := target_year || target_month_2c || target_day_2c;

    iamrole_string varchar(255) := 'arn:aws:iam::564512845791:role/sdlf-engineering-AWSRedshiftRole';

    output_filename varchar(255) :=     folder_name || file_sequential_name || '_itau_ativos_e_inativos';

    select_string varchar(40) :=        'SELECT * FROM #Mensalidade';
    unload_string varchar(1000) :=      'UNLOAD ('           || QUOTE_LITERAL(select_string)        || ')' ||
                                            ' TO '              || QUOTE_LITERAL(output_filename)   ||
                                            'IAM_ROLE'          || QUOTE_LITERAL(iamrole_string)                ||
                                            'DELIMITER '        || QUOTE_LITERAL(',')                           ||
                                            'HEADER
                                            ALLOWOVERWRITE
                                            PARALLEL OFF
                                            MAXFILESIZE AS 80 MB
                                            EXTENSION '         || QUOTE_LITERAL('csv');

BEGIN

    -- MENSALIDADE
    DROP TABLE IF EXISTS #Mensalidade_Base;
    SELECT
        DISTINCT AnoReferencia
        ,MesReferencia
        ,CASE
            WHEN CodigoCampanha ILIKE '%OfertaItau%' THEN CodigoCampanha
            ELSE 'Tombados'
        END AS CodigoCampanha
        ,BinCartao
        ,Tagid
        ,CASE 
            WHEN DataPedido <'20211001' THEN '20211001'
            ELSE DataPedido
        END AS DataPedido
        ,DataAtivacao_Oferta AS DataAtivacao
        ,Plano
        ,StatusTag
        ,CASE
            WHEN DataUltimaTransacao < DataAtivacao_Oferta THEN DataAtivacao_Oferta
            ELSE DataUltimaTransacao
        END AS DataUltimaTransacao
        ,CanalAquisicao
        ,Remuneracao
        ,Documento
        ,ROW_NUMBER() OVER (PARTITION BY Tagid ORDER BY datapedido DESC) AS rn
    INTO
        #Mensalidade_Base
    FROM
        refined.main.tb_historico_itau_cobranded
    WHERE
        CheckFaturamento = 1
        AND anoreferencia = DATE_PART(year, CURRENT_DATE - INTERVAL '1 month')
        AND mesreferencia = DATE_PART(month, CURRENT_DATE - INTERVAL '1 month');


    -- Cria uma nova tabela temporária sem duplicidades;
    DROP TABLE IF EXISTS #Mensalidade;
    SELECT
        *
    INTO
        #Mensalidade
    FROM
        #Mensalidade_Base
    WHERE
        rn = 1;
    

    -- Executa o comando de UNLOAD p/ S3 
    EXECUTE unload_string;


    -- Salva histórico de execução
    INSERT INTO refined.main.tb_itau_historico_staff VALUES (output_filename, 'Base Cobranded', getdate());
    exit_msg = 'Arquivo criado no S3 com sucesso';


    --limpa tabelas temporárias
    DROP TABLE IF EXISTS #Mensalidade_Base;
    DROP TABLE IF EXISTS #Mensalidade;

END
$$