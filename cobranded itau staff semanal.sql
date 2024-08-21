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
        AND anoreferencia = DATE_PART(year, CURRENT_DATE)
        AND mesreferencia = DATE_PART(month, CURRENT_DATE);


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
    UNLOAD ('SELECT * FROM #Mensalidade')

    TO 's3://cntcar-dlk-dev-us-east-2-564512845791-data-analytics/compartilhamento-areas-de-negocio/financas/staff itau/20240820_itau_ativos_e_inativos'
    iam_role 'arn:aws:iam::564512845791:role/sdlf-engineering-AWSRedshiftRole'
    CSV delimiter ';'
    HEADER
    ALLOWOVERWRITE
    PARALLEL OFF
    MAXFILESIZE AS 80 MB
    EXTENSION 'csv'
    ;

    
    --limpa tabelas temporárias
    DROP TABLE IF EXISTS #Mensalidade_Base;
    DROP TABLE IF EXISTS #Mensalidade;