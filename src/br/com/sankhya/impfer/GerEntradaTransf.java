package br.com.sankhya.impfer;

import br.com.sankhya.extensions.eventoprogramavel.EventoProgramavelJava;
import br.com.sankhya.jape.core.JapeSession;
import br.com.sankhya.jape.event.PersistenceEvent;
import br.com.sankhya.jape.event.TransactionContext;
import br.com.sankhya.jape.sql.NativeSql;
import br.com.sankhya.jape.vo.DynamicVO;
import br.com.sankhya.jape.wrapper.JapeFactory;
import br.com.sankhya.jape.wrapper.JapeWrapper;
import br.com.sankhya.modelcore.comercial.AtributosRegras;
import com.sankhya.util.BigDecimalUtil;

import java.math.BigDecimal;
import java.util.Collection;

public class GerEntradaTransf implements EventoProgramavelJava {

    private static final BigDecimal TOP_TRANSF_SAIDA = BigDecimal.valueOf(1152);
    private static final BigDecimal TOP_TRANSF_ENTRADA = BigDecimal.valueOf(1452);

    // Instâncias de JapeWrapper para interagir com as tabelas do Sankhya
    private JapeWrapper cabDAO = JapeFactory.dao("CabecalhoNota");
    private JapeWrapper iteDAO = JapeFactory.dao("ItemNota");

    @Override
    public void beforeInsert(PersistenceEvent event) throws Exception {

    }

    @Override
    public void beforeUpdate(PersistenceEvent event) throws Exception {

    }

    @Override
    public void beforeDelete(PersistenceEvent event) throws Exception {

    }

    @Override
    public void afterInsert(PersistenceEvent event) throws Exception {

    }

    @Override
    public void afterUpdate(PersistenceEvent event) throws Exception {

        boolean confirmando = JapeSession.getProperty(AtributosRegras.CONFIRMANDO) != null;

        DynamicVO cabVO = (DynamicVO) event.getVo();

        BigDecimal codtipoper = cabVO.asBigDecimal("CODTIPOPER");
        String statusnfe = cabVO.asString("STATUSNFE");
        BigDecimal nunota = BigDecimal.ZERO;
        BigDecimal codemp = cabVO.asBigDecimal("CODPARC");
        BigDecimal codparc = cabVO.asBigDecimal("CODEMP");
        BigDecimal numnota = cabVO.asBigDecimal("NUMNOTA");

        if (codtipoper.equals(TOP_TRANSF_SAIDA) && statusnfe.equals("A") && confirmando) {

            BigDecimal existEntrada = NativeSql.getBigDecimal("COUNT(1)", "TGFCAB", "NUMNOTA=? AND CODEMP=? AND CODPARC=? AND  TIPMOV = 'C'", new Object[]{numnota, codemp, codparc});

            if(BigDecimalUtil.isNullOrZero(existEntrada)) {

                nunota = gerarEntradaTransf(cabVO);

                if (!BigDecimalUtil.isNullOrZero(nunota)) {

                    DynamicVO transfVO = JapeFactory.dao("CabecalhoNota")
                            .findOne("NUNOTA=?", new Object[]{cabVO.asBigDecimal("NUNOTA")});

                    cabDAO.prepareToUpdateByPK(transfVO.asBigDecimal("NUNOTA"))
                            .set("NUREM", nunota)
                            .update();


                }


            }

        }


    }

    @Override
    public void afterDelete(PersistenceEvent event) throws Exception {

    }

    @Override
    public void beforeCommit(TransactionContext tranCtx) throws Exception {

    }

    public BigDecimal gerarEntradaTransf(DynamicVO cabecalhoVO) throws Exception {


        System.out.println("##### Inserindo Cabeçalho da entrada Nr NF " + cabecalhoVO.asBigDecimal("NUMNOTA"));

        DynamicVO newCabecalho = cabDAO.create()
                .set("CODEMP", cabecalhoVO.asBigDecimal("CODPARC"))
                .set("CODEMPNEGOC", cabecalhoVO.asBigDecimal("CODPARC"))
                .set("NUMNOTA", cabecalhoVO.asBigDecimal("NUMNOTA"))
                .set("CODPARC", cabecalhoVO.asBigDecimal("CODEMP"))
                .set("CODVEND",cabecalhoVO.asBigDecimal("CODVEND"))
                .set("CODNAT", BigDecimal.valueOf(99001000))
                .set("STATUSNOTA","A")
                .set("DTNEG", cabecalhoVO.asTimestamp("DTNEG"))
                .set("NUREM", cabecalhoVO.asBigDecimal("NUNOTA"))
                .set("DTMOV", cabecalhoVO.asTimestamp("DTMOV"))
                .set("DTFATUR", cabecalhoVO.asTimestamp("DTFATUR"))
                .set("CODTIPOPER", TOP_TRANSF_ENTRADA)
                .set("CODCENCUS", BigDecimal.valueOf(101007))
                .set("CODTIPVENDA",cabecalhoVO.asBigDecimal("CODTIPVENDA"))
                .set("SERIENOTA",cabecalhoVO.asString("SERIENOTA"))
                .set("CHAVENFE",cabecalhoVO.asString("CHAVENFE"))
                .set("VLRNOTA",cabecalhoVO.asBigDecimal("VLRNOTA"))
                .set("OBSERVACAO", "Entrada de Transferência gerada automaticamente Pela NF de Transferencia de Nr unico " + cabecalhoVO.asBigDecimal("NUNOTA"))
                .save();

        BigDecimal nunota = newCabecalho.asBigDecimal("NUNOTA");
        inserirItens(nunota,cabecalhoVO.asBigDecimal("NUNOTA"));

        return nunota;

    }

    private void inserirItens(BigDecimal nunota,BigDecimal nunotav) throws Exception {

        System.out.println("Inserindo Itens Nr NF " + nunota.toString());

        Collection<DynamicVO> itensVO = JapeFactory.dao("ItemNota")
                .find("NUNOTA=?", new Object[]{nunotav});

        for (DynamicVO iteVO : itensVO) {

            DynamicVO ite  = iteDAO.create()
                    .set("NUNOTA", nunota)
                    .set("CODPROD", iteVO.asBigDecimal("CODPROD"))
                    .set("CODLOCALORIG", iteVO.asBigDecimal("CODLOCALORIG"))
                    .set("QTDNEG", iteVO.asBigDecimal("QTDNEG"))
                    .set("ATUALESTOQUE",BigDecimal.valueOf(1))
                    .set("RESERVA","N")
                    .set("VLRUNIT", iteVO.asBigDecimal("VLRUNIT"))
                    .set("VLRTOT",iteVO.asBigDecimal("VLRTOT"))
                    .set("BASEIPI",iteVO.asBigDecimal("BASEIPI"))
                    .set("BASEICMS",iteVO.asBigDecimal("BASEICMS"))
                    .set("CODVOL",iteVO.asString("CODVOL"))
                    .save();

        }


    }

}
