WITH base_porto AS (
    SELECT
        DISTINCT c.CodigoInternoParceiro CPF,
        ac.AdesaoId
    FROM
        stage.main.tb_parceiro_dbo_contrato c
        JOIN stage.main.tb_parceiro_dbo_ativacaocontrato ac ON ac.ContratoId = c.ContratoId
    WHERE
        c.ParceiroId = 1
)
SELECT
    --Transacoes
    date_trunc('month', t.data) :: DATE AS Data,
    count(DISTINCT t.transacaoid) AS qtdTransacoes,
    SUM(t.valor + valordesconto) AS vlrTransacoes,
    count(DISTINCT a.adesaoid) AS qtdAdesoes,
    --te.tarifadeinterconexaopreciso,
    gpn.nome AS GrupoConveniado,
    pn.nomefantasia,
    --pn.razaosocial,
    --pn.codigoerp,
    --Cliente
    --base.cpf CPFs,
    po.agregado,
    po.detalhado
FROM
    stage.main.tb_conectcar_dbo_transacao t
    INNER JOIN stage.main.tb_conectcar_dbo_transacaoestacionamento te ON te.transacaoid = t.transacaoid
    AND te.datareferencia >= '2024-01-01'
    LEFT JOIN stage.main.tb_conectcar_dbo_parceironegocio pn ON pn.parceironegocioid = te.conveniadoid
    LEFT JOIN stage.main.tb_conectcar_dbo_grupoparceironegocio gpn ON gpn.grupo_parceiro_negocio_id = pn.grupoparceironegocioid --Informações sobre o cliente
    INNER JOIN stage.main.tb_conectcar_dbo_adesao a ON a.adesaoid = t.adesaoid
    INNER JOIN refined.main.tb_redshift_ciclodevida_ativacao at ON at.adesaoid = a.adesaoid
    LEFT JOIN refined.main.tb_redshift_ciclodevida_portifolio po ON po.adesaoid = t.Adesaoid
    /*left join base_porto base 
     ON base.adesaoid = t.adesaoid*/
WHERE
    t.data >= '2024-01-01'
    AND upper(gpn.nome) IN ('MULTIPLAN')
    AND upper(po.agregado) IN ('PORTO SEGURO')
    AND po.detalhado NOT ILIKE 'CARRO%FACIL'
GROUP BY
    t.valor + valordesconto,
    --te.tarifadeinterconexaopreciso,
    date_trunc('month', t.data),
    pn.nomefantasia,
    --pn.razaosocial,
    --pn.codigoerp,
    gpn.nome,
    --base.cpf,
    po.agregado,
    po.detalhado;


--CHECK MAIO/JUNHO
SELECT
    DISTINCT DATE_TRUNC('MONTH', data) :: date
FROM
    stage.main.tb_conectcar_dbo_transacao t
    INNER JOIN stage.main.tb_conectcar_dbo_transacaoestacionamento te ON te.transacaoid = t.transacaoid
    LEFT JOIN stage.main.tb_conectcar_dbo_parceironegocio pn ON pn.parceironegocioid = te.conveniadoid
WHERE
    pn.grupoparceironegocioid = 30
    AND DATE_TRUNC('MONTH', data) > '2024-04-01'
ORDER BY
    data;