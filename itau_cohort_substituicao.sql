-- INDICADORES ITÁU

    --importação da tabela de adesão referente ao periodo e com o tipo de substituição
    DROP TABLE IF EXISTS #ativacao;
    SELECT
        a.*
    INTO #ativacao
    FROM
        refined.main.tb_redshift_ciclodevida_ativacao a (NOLOCK)
        JOIN refined.main.tb_redshift_ciclodevida_cliente c (NOLOCK) ON c.Clienteid = a.CLIENTEID
    WHERE
        c.PessoaFisica = 1;


	--SUBSTITUIÇÃO
    DROP TABLE IF EXISTS #substituicao;
    SELECT 
        *,
        CAST(DATE_TRUNC('month', datapedido) AS DATE) MesReferencia
    INTO #substituicao
    FROM refined.main.tb_historico_itau_substituicao;


    --data ativacao
    DROP TABLE IF EXISTS #DataAtivacao;
    SELECT
        CAST(DATE_TRUNC('month', a.DATA) AS DATE) AS DataAtivacao,
        a.ADESAOID 
    INTO #DataAtivacao
    FROM
        #ativacao a
    WHERE
        a.AdesaoOrigemId IS NULL;


    -- chumbado aqui areawork.dbo.IndicadoresCarteira_Cancelamento
    DROP TABLE IF EXISTS #CobrandedFinal;
    SELECT
        datareferencia AS _MesReferencia,
        CASE
            WHEN CodigoCampanha LIKE '%OfertaIta%' THEN 'LP Tag Itaú'
            ELSE 'Tombados'
        END AS Origem,
        ibf.Documento,
        ibf.PrimeiraADesaoId,
        ibf.TagId,
        ibf.AdesaoId,
        ibf.Plano,
        ibf.StatusTag,
        a.DataAtivacao AS DataAtivacaoTag 
    INTO #CobrandedFinal
    FROM
        refined.main.tb_historico_itau_cobranded ibf
        JOIN #DataAtivacao a
        ON a.ADESAOID = ibf.PrimeiraAdesaoId
    WHERE
        checkfaturamento = 1
        AND _MesReferencia >= '20230301'
        AND _MesReferencia < CURRENT_DATE - INTERVAL '1 day';


    --SUBSTITUIÇÃO ITAU
    SELECT
        M._MesReferencia AS SafraAtivacao,
        CASE
            WHEN ('M' + lpad(datediff(MONTH,M._MesReferencia,c.datapedido),2,'0')) IS NULL THEN 'Sem substituição'
            ELSE ('M' + lpad(datediff(MONTH,M._MesReferencia,c.datapedido),2,'0'))
        END AS MesesCancelamento,
        COUNT(DISTINCT M.primeiraAdesaoId) AS SafraEntrada,
        COUNT(DISTINCT c.primeiraadesaoid) AS Substituicoes
    FROM
        #CobrandedFinal M
        LEFT JOIN #substituicao c
        ON c.PrimeiraAdesaoId = M.PrimeiraADesaoId
        AND c.datapedido < DATE_TRUNC('month', CAST(GETDATE() AS DATE))
        AND c.MesReferencia > M._MesReferencia
    WHERE
        M._MesReferencia = DataAtivacaoTag
        AND M._MesReferencia >= '20230301'
        AND M._MesReferencia < CAST(GETDATE() AS DATE)
    GROUP BY
        M._MesReferencia,
        MesesCancelamento
    ORDER BY
        M._MesReferencia,
        MesesCancelamento;