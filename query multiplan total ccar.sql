SELECT
    DISTINCT 
    date_trunc('day', datahoraentrada):: date AS Data,
    t.transacaoid transacaoid,
    t.valor valor,
    a.adesaoid adesaoid,
    'transacao' origem
FROM
    stage.main.tb_conectcar_dbo_transacaoestacionamento te
    INNER JOIN stage.main.tb_conectcar_dbo_transacao t ON te.transacaoid = t.transacaoid
    LEFT JOIN stage.main.tb_conectcar_dbo_conveniado co ON co.conveniado_id = te.conveniadoid
    LEFT JOIN stage.main.tb_conectcar_dbo_parceironegocio pn ON pn.parceironegocioid = te.conveniadoid
    INNER JOIN stage.main.tb_conectcar_dbo_adesao a ON a.adesaoid = t.adesaoid
    --INNER JOIN #base_porto bp ON bp.AdesaoId = a.adesaoid
WHERE
    pn.GrupoParceiroNegocioId = 30
    AND datahoraentrada >= '20240101'

UNION ALL 

SELECT DISTINCT
    date_trunc('day', sa.dataentrada) :: date AS data,
    sa.transacaoid transacaoid,
    sa.valor valor,
    sa.adesaoid adesaoid,
    'autorizacao' origem
FROM
    "stage"."main"."tb_autorizador_dbo_solicitacaoautorizacao" sa
    LEFT JOIN "stage"."main"."tb_conectcar_dbo_parceironegocio" pn ON pn.parceironegocioid = sa.conveniadoid
    LEFT JOIN "stage"."main"."tb_conectcar_dbo_adesao" ad ON ad.adesaoid = sa.adesaoid
    --INNER JOIN #base_porto base ON base.adesaoid = ad.adesaoid
WHERE
    pn.GrupoParceiroNegocioId = 30
    AND sa.dataentrada >= '20240101'
;