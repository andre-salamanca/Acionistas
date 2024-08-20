--importação da tabela de adesão referente ao periodo 
drop table if exists #ativacao;
select 
    a.AdesaoId,
	a.AdesaoOrigemId,
	a.Data,
	a.PrimeiraAdesaoId,
	a.NomePlano,
	a.TaxaAdesaoMensalidade,
	case when a.datacancelamento >= CAST(getdate() as date) then null else a.datacancelamento end dataCancelamento,
	a.documento,
	a.clienteId,
    a.CodigoNFC,
	a.TagId
into #ativacao 
from refined.main.tb_redshift_ciclodevida_ativacao a 
join dev.stage.vw_conectcar_dbo_cliente c on c.Clienteid = a.CLIENTEID
where 
    c.PessoaFisica = 1 
    and a.Data < CAST(getdate() as date);
    
--pegar relacao cliente documento (usa varias vezes ao longo do código)
drop table if exists #clientedocumento;
select distinct 
    clienteId,
    documento 
into #clientedocumento 
from #ativacao;

--historica cartaocreedito 
/*drop table if exists #DCC;
select 
    datadecadastro,
    clienteId,
    cartaoBIN,
    dadoscartaocreditoId 
into #DCC
from dev.stage.vw_conectcar_dbo_dadoscartaocredito where datadecadastro < CAST(getdate() as date);
*/
--pvi 
drop table if exists #PVI;
select	
    Numero_pedido as NumeroPedido, 
    CPF,
    codigo_campanha as CodigoCampanha,
    Adesao_substituida_id as AdesaoSubstituidaId,
    data_pedido as datapedido,
	origem_cliente as origemcliente,
    canal_aquisicao as canalaquisicao,
    valor_em_centavos as  ValorEmCentavos,
    valor_taxa_substituicao as ValorTaxaSubstituicao,
    pedido_venda_id as PedidoVendaId
into #PVI
from dev.stage.vw_conectcar_dbo_pedidos_pedidovendaimportado 
where 
    codigocampanha ilike '%OfertaItauVi%'
	and status_pedido = 6 
    and data_pedido < cast(getdate() as date);
--PV
drop table if exists #PV;
select 
    pedidoVendaId,
    CPF,
    isnull(convert(integer,substituicao),0) as Substituicao,
    vendaTagPrecoVenda 
into #PV
from dev.stage.vw_conectcar_dbo_pedidovenda;

--integracao pedido
drop table if exists #IP;
select 
    origemPedidoId, 
    numeropedidoOrigem 
into #IP
from dev.stage.vw_CONTROLLOOP_DBO_INTEGRACAOPEDIDO
where origempedidoId in (5,18);

--dados cartao tokenizacao (gateway pagamento)
drop table if exists #gatewaypagamentotokenizacao;
select 
    CLIENTEID,
    Identificador,
    BinCartao,
    FormaDePagamento,
    Data 
into #gatewaypagamentotokenizacao  
from stage.main.tb_gatewaypagamento_dbo_tokenizacao 
join #clientedocumento cd on cd.Documento =  LTRIM( IdentificadorDoCliente,'0')
where 
    DescricaoRetornoDoGateway = 'Approved' 
    and Data<CAST(getdate() as date);

--dados cartao credito (sys)
drop table if exists #DadosCartaoCredito;
select 
    clienteid,
    DadosCartaoCreditoId,
    cartaobin,
    'CartaoDeCredito' as formadepagamento,
    datadecadastro
into #DadosCartaoCredito 
from dev.stage.vw_conectcar_dbo_dadoscartaocredito where datadecadastro < CAST(getdate() as date);

--pedidos aprovados (todos)
drop table if exists #pedidos;
select 
	pvi.numeropedido,
	cast(pvi.dataPedido as date) as DataPedido,
	case when pvi.CodigoCampanha like '%OfertaItauVitalicia%' then  pvi.CodigoCampanha
		 when pvi.CodigoCampanha like  '%financiamento%' then pvi.CodigoCampanha
	else 'Tombado' end CodigoCampanha,
	isnull (ltrim(pvi.cpf,'0'),ltrim(replace(replace(pv.CPF,'.',''),'-',''),'0')) as Documento,
	pvi.Valoremcentavos/100.0 ValorPedido,
	pvi.adesaoSubstituidaId as adesaoSubstituidaId_Pedido,
	case when pv.vendaTagPrecoVenda > 0 then 0 else 1 end SubstituicaoIsenta,
	ove.NFC,
	pvi.CanalAquisicao,
	pvi.ValorTaxaSubstituicao,
	pv.Substituicao,
	cp.OrigemPedidoId
into #Pedidos
from #PVI pvi
left join dev.refined.vw_redshift_ciclodevida_origemvendaweb ove on ove.numeroPedido = pvi.NumeroPedido
left join #PV pv on pv.PedidoVendaId = pvi.PedidoVendaId
left join #IP CP on CP.NumeroPedidoOrigem = pvi.Numeropedido;

--pegando dados do contrato (primeiraAdesaoId) marcando origem e ativação
drop table if exists #Origem;
select 
	a1.primeiraadesaoid,
	case when cast(a1.Data as date)<'20211001' then '20211001' else cast(a1.Data as date) end as DataAtivacao,
	isnull(p.CodigoCampanha,aa.NomeCampanha) as CodigoCampanha,
	p.DataPedido
into #Origem
from #ativacao a1
left join #Pedidos p on upper(a1.CodigoNFC) = upper(p.NFC)
left join refined.main.tb_redshift_auxciclodevida_acoesavulsas aa on upper(aa.CodigoNFC) = upper(a1.CodigoNFC)
	and aa.NomeCampanha ilike '%RockInRio%'
where a1.AdesaoOrigemId is null;

drop table if exists #origem_adesao;
select distinct 
    a.ADESAOID,
    a.primeiraadesaoid,
    a.CLIENTEID,a.Documento, 
	case when o.CodigoCampanha ilike '%vitali%' then 'LP Tag Itau' else 'Tombado' end Origem
into #origem_adesao 
from  #ativacao a  left join #Origem o on o.primeiraadesaoid = a.primeiraadesaoid;


-- substituicao da Tag Itaú  (tudo que não vem de RedVentures)
drop table if exists #origemPedido;
select distinct p.*, 
	(select top 1 AdesaoOrigemId from #ativacao a where upper(a.CodigoNFC) = upper(p.nfc) 
	and a.DATA >= p.DataPedido order by ADESAOID asc) AdesaoId_substituida_real,
	(select top 1 ADESAOID from #ativacao a where upper(a.CodigoNFC) = upper(p.nfc) 
	and a.DATA >= p.DataPedido order by ADESAOID asc) AdesaoId_nova_real
into #origemPedido
from #Pedidos p
where CodigoCampanha ilike '%OfertaIta%' 
	and canalaquisicao not ilike '%RV%' 
    and substituicao = 1 and origemPedidoId in (5,18) 
	and DataPedido < CAST(getdate() as date)
	--and DataPedido < DATE_TRUNC('month', CURRENT_DATE)::DATE
	and DataPedido >= date_trunc('month',dateadd(day,-1,getdate()));
	--and DataPedido >= DATE_TRUNC('month', CURRENT_DATE - INTERVAL '1 month');

--casos em que há mais de uma Tag (nfc) por pedido, pega o primeiro NFC desc (pega o ativado apenas, se houver)
drop table if exists #origempedido2;
select distinct op3.* into #origempedido2 from #origemPedido op3 join 
(select distinct numeroPedido,
(select top 1 NFC from #origempedido op2 where op2.numeropedido = op1.numeropedido order by AdesaoId_substituida_real desc) NFC
from #origempedido op1) op4 on 
op4.numeroPedido = op3.numeroPedido and isnull(upper(op3.NFC),'0') = isnull(upper(op4.NFC),'0');

-- dados cartao
--lista de bins elegíveis para a oferta
drop table if exists #BINsItau;
SELECT distinct valor as bin into #BINsItau
FROM stage.main.tb_conectcar_oferta_criteriosoferta
WHERE oferta_id  = 315 and tipo = 'Bin';
--pegar cartão cadastrado antes do pedido (tokenizacao)


drop table if exists #Pedido_Cartao_tk;
select 
	op.numeroPedido,op.Documento,'TK' as fonte, 
	MAX(gp.Identificador) as Identificador_ult
into #Pedido_Cartao_tk
from #origempedido2 op
join #clientedocumento cd on cd.documento = op.Documento
join #gatewaypagamentotokenizacao  gp on gp.CLIENTEID = cd.CLIENTEID 
and gp.Data <= op.DataPedido
group by op.numeroPedido,op.Documento;

--pegar cartão cadastrado antes do pedido (dadoscartaocredito)
drop table if exists #Pedido_Cartao_cc;
select 
	op.NumeroPedido,op.Documento,'CC' as fonte, 
	MAX(dcc.DadosCartaoCreditoId) as DadosCartaoCreditoId_ult
into #Pedido_Cartao_cc
from #origemPedido2 op
join #clientedocumento cd on cd.documento = op.Documento
join #DadosCartaoCredito dcc on dcc.CLIENTEID = cd.CLIENTEID 
and dcc.DataDeCadastro <= op.DataPedido
group by op.NumeroPedido,op.Documento;

--unir dados --pegar primeiro tokenizacao
drop table if exists #Pedido_Cartao_Final;
select 
	op.NumeroPedido,gp.BinCartao into #Pedido_Cartao_Final
from #origempedido2 op
join #Pedido_Cartao_tk tk on tk.numeroPedido = op.numeropedido
join #gatewaypagamentotokenizacao gp on gp.Identificador = tk.Identificador_ult;

--inserir o que nao foi encotnrato no outro
insert into #Pedido_Cartao_Final
select op.numeroPedido, dcc.CartaoBIN FROM #origempedido2 op
join #Pedido_Cartao_cc cc on cc.numeropedido = op.numeropedido
join #DadosCartaoCredito dcc on dcc.DadosCartaoCreditoId = cc.DadosCartaoCreditoId_ult
where op.numeroPedido not in (select numeropedido from #Pedido_Cartao_Final);


drop table if exists #substituicao;
select distinct
	p.*,
	cast(a2.Data as date) as DataAtivacao,
	oa.Origem,
	oa.primeiraadesaoid,
	pc.BinCartao,
	case when bin.BIN is not null then 'Adicionado' else 'Removido' end Oferta,
	case when (
		p.adesaoSubstituidaId_Pedido is null or 
		bin.BIN is null 
		or canalAquisicao like '%Atendimento%' or SubstituicaoIsenta = 0
		) 
		then 0 else 1 end CheckFaturamento,
	case when canalAquisicao like '%Atendimento%' then 'atendimento'
		 when SubstituicaoIsenta = 0 then 'Pago Pelo Cliente'
		 when p.adesaoSubstituidaId_Pedido is null then 'sem adesaosubstituidaId'
		 when bin.BIN is null then 'cartão inválido'
		 else 'OK' end RegraExclusao
into #substituicao
from #origemPedido2 p
left join #origem_adesao oa on oa.ADESAOID = p.adesaoSubstituidaId_pedido
left join #ativacao a on a.Documento = p.Documento
left join #ativacao a2 on p.AdesaoId_nova_real = a2.ADESAOID
left join #Pedido_Cartao_Final pc on pc.numeropedido = p.NumeroPedido
left join #BINsItau bin on bin.BIN = pc.BinCartao;

--jogar na temp para testar
drop table if exists #tb_historico_itau_substituicao;
select * into #tb_historico_itau_substituicao
from stage.main.tb_historico_itau_substituicao;

--pegar o atual e juntar com o passado pra fazer as validações
delete from  #tb_historico_itau_substituicao
where 
	Ano =  date_part(year,(DATEADD(day,-1,getdate())))
	and Mes =  date_part(month,(DATEADD(day,-1,getdate())));
	--and Mes = 10;

drop table if exists #substituicao_;
select 
	numeroPedido,
	DataPedido,
	RegraExclusao,
	primeiraadesaoid 
into #substituicao_
from #substituicao;

insert into #substituicao_
select 
	NumeroPedido,
	datapedido,
	CheckFaturamento,
	primeiraadesaoid 
from #tb_historico_itau_substituicao;

--=====fazer tratamento do valor a ser pago pelo Itaú======

--regra: o itaú paga a substituição quando o cliente não pagar,desde que seja uma substituiççao a cada 12 M

--registrar as que não vão pro faturamento
drop table if exists #substituicao2;
select 
	NumeroPedido,
	'--' as ValorItau,
	RegraExclusao
into #substituicao2 
from #substituicao_ 
where RegraExclusao <> 'OK';

--pegar as pagas isenta = 0 (itau não remunera)
--verifica se tem um pedido isento nos ultimos meses, se tiver, o itau paga 0 se não, 20 (isnull)
drop table if exists  #Subst;
select 
	s.NumeroPedido,
	(
		select
			top 1 numeropedido
		from
			#substituicao_ s2
		where
			s2.RegraExclusao = 'OK'
			and s.primeiraadesaoid = s2.primeiraadesaoid
			and s2.DataPedido >= DATEADD(month,-12,s.DataPedido)
			and s.NumeroPedido > s2.NumeroPedido
		order by
			s2.numeropedido desc
	) as ValorItau
into #subst
from #substituicao_ s 
where  RegraExclusao = 'OK';

insert into #Substituicao2
select numeropedido,
case when valorItau is null then '08' else '00' end ValorItau,
case when ValorItau is null then 'OK' else 'Segundo Pedido Isento' end
 from #subst;


--juntar tudo de novo
drop table if exists #substituicao3;
select 		
	s1.*,
	case when s2.ValorItau = '--' then null
		 when s2.ValorItau = '00' then 0
		 when s2.ValorItau IN ('20', '08') then 8 
		 else  -1 end ValorItau,
	s2.RegraExclusao as RegraExclusaoGeral
into #substituicao3
from #substituicao s1
join #substituicao2 s2 on s1.NumeroPedido = s2.NumeroPedido;


-- relatorio final 
drop table if exists #substituicao4;
select distinct
	date_part(year,(DATEADD(day,-1,getdate()))) as Ano,
	date_part(month,(DATEADD(day,-1,getdate()))) as Mes,
	--10 as Mes,
	s.BinCartao,
	a.Tagid as TagIdAnterior,
	s.DataPedido,
	a2.Tagid as TagIdNova,
	CAST(a2.Data as date) as DataAtivacao,
	s.Origem,
	s.primeiraadesaoid,
	s.NumeroPedido,
	s.ValorItau,
	s.SubstituicaoIsenta,
	s.RegraExclusaoGeral as CheckFaturamento,
	getdate() as dataProcessamento
into #substituicao4
from #substituicao3 s
left join #ativacao a on s.adesaoSubstituidaId_Pedido = a.ADESAOID
left join #ativacao a2 on s.AdesaoId_nova_real = a2.ADESAOID;


-- apaga dados do mês atual
select *   from #substituicao4;