package br.com.sankhya.impfer;

import br.com.sankhya.extensions.actionbutton.AcaoRotinaJava;
import br.com.sankhya.extensions.actionbutton.ContextoAcao;
import br.com.sankhya.extensions.actionbutton.Registro;
import br.com.sankhya.jape.EntityFacade;
import br.com.sankhya.jape.dao.JdbcWrapper;
import br.com.sankhya.jape.sql.NativeSql;
import br.com.sankhya.jape.vo.DynamicVO;
import br.com.sankhya.jape.wrapper.JapeFactory;
import br.com.sankhya.jape.wrapper.JapeWrapper;
import br.com.sankhya.modelcore.util.EntityFacadeFactory;
import com.sankhya.util.BigDecimalUtil;

import java.math.BigDecimal;
import java.util.Collection;


public class GerarEntradaTrasfAcao implements AcaoRotinaJava {


    private static final BigDecimal TOP_TRANSF_SAIDA = BigDecimal.valueOf(1152);
    private static final BigDecimal TOP_TRANSF_ENTRADA = BigDecimal.valueOf(1452);

    // Instâncias de JapeWrapper para interagir com as tabelas do Sankhya
    private JapeWrapper cabDAO = JapeFactory.dao("CabecalhoNota");
    private JapeWrapper iteDAO = JapeFactory.dao("ItemNota");
    @Override
    public void doAction(ContextoAcao arg0) throws Exception {


        EntityFacade dwf = EntityFacadeFactory.getDWFFacade();
        final JdbcWrapper jdbc = dwf.getJdbcWrapper();
        BigDecimal vnunota;
        Registro[] linhas = arg0.getLinhas();

        for (Registro linha : linhas) {

            vnunota = (BigDecimal) linha.getCampo("NUNOTA");

            BigDecimal codtipoper = (BigDecimal) linha.getCampo("CODTIPOPER");
            String statusnota = linha.getCampo("STATUSNOTA").toString();
            String statusnfe = linha.getCampo("STATUSNFE").toString();
            BigDecimal codemp = (BigDecimal) linha.getCampo("CODPARC");
            BigDecimal codparc = (BigDecimal) linha.getCampo("CODEMP");
            BigDecimal numnota = (BigDecimal) linha.getCampo("NUMNOTA");

            BigDecimal nunota = BigDecimal.ZERO;

            BigDecimal existEntrada = NativeSql.getBigDecimal("COUNT(1)","TGFCAB","NUMNOTA=? AND TIPMOV = 'C' AND CODEMP=? AND CODPARC=?",new Object[]{numnota,codemp,codparc});


            if (existEntrada.equals(BigDecimal.ZERO) &&
                    statusnota.equals("L") &&
                    codtipoper.equals(TOP_TRANSF_SAIDA)) {

                nunota = gerarEntradaTransf(linha);

                if(!BigDecimalUtil.isNullOrZero(nunota)) {

                    DynamicVO vendaVO = JapeFactory.dao("CabecalhoNota")
                            .findOne("NUNOTA=?", new Object[]{vnunota});

                    cabDAO.prepareToUpdateByPK(vendaVO.asBigDecimal("NUNOTA"))
                            .set("NUREM", nunota)
                            .update();
                }


            }




        }


    }


    public BigDecimal gerarEntradaTransf(Registro linha) throws Exception {


        System.out.println("##### Inserindo Cabeçalho da entrada Nr NF " + linha.getCampo("NUMNOTA"));

        DynamicVO newCabecalho = cabDAO.create()
                .set("CODEMP", linha.getCampo("CODPARC"))
                .set("CODEMPNEGOC", linha.getCampo("CODPARC"))
                .set("NUMNOTA", linha.getCampo("NUMNOTA"))
                .set("CODPARC", linha.getCampo("CODEMP"))
                .set("CODVEND",linha.getCampo("CODVEND"))
                .set("CODNAT", BigDecimal.valueOf(99001000))
                .set("STATUSNOTA","A")
                .set("DTNEG", linha.getCampo("DTNEG"))
                .set("NUREM", linha.getCampo("NUNOTA"))
                .set("DTMOV", linha.getCampo("DTMOV"))
                .set("DTFATUR", linha.getCampo("DTFATUR"))
                .set("CODTIPOPER", TOP_TRANSF_ENTRADA)
                .set("CODCENCUS", BigDecimal.valueOf(101007))
                .set("CODTIPVENDA",linha.getCampo("CODTIPVENDA"))
                .set("SERIENOTA",linha.getCampo("SERIENOTA"))
                .set("CHAVENFE",linha.getCampo("CHAVENFE"))
                .set("VLRNOTA",linha.getCampo("VLRNOTA"))
                .set("OBSERVACAO", "Entrada de Transferência gerada automaticamente Pela NF de Transferencia de Nr unico " + linha.getCampo("NUNOTA"))
                .save();

        BigDecimal nunota = newCabecalho.asBigDecimal("NUNOTA");
        inserirItens(nunota, (BigDecimal) linha.getCampo("NUNOTA"));

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
