package br.com.sankhya.impfer;

import br.com.sankhya.jape.EntityFacade;
import br.com.sankhya.jape.dao.JdbcWrapper;
import br.com.sankhya.jape.sql.NativeSql;
import br.com.sankhya.jape.util.JapeSessionContext;
import br.com.sankhya.jape.vo.DynamicVO;
import br.com.sankhya.jape.wrapper.JapeFactory;
import br.com.sankhya.jape.wrapper.JapeWrapper;
import br.com.sankhya.modelcore.auth.AuthenticationInfo;
import br.com.sankhya.modelcore.comercial.CentralFinanceiro;
import br.com.sankhya.modelcore.comercial.CentralItemNota;
import br.com.sankhya.modelcore.comercial.impostos.ImpostosHelpper;
import br.com.sankhya.modelcore.dwfdata.vo.ItemNotaVO;
import br.com.sankhya.modelcore.util.EntityFacadeFactory;
import br.com.sankhya.modelcore.util.MGECoreParameter;
import com.sankhya.util.BigDecimalUtil;
import org.cuckoo.core.ScheduledAction;
import org.cuckoo.core.ScheduledActionContext;

import java.math.BigDecimal;
import java.sql.ResultSet;
import java.util.Collection;

public class GerarEntTransfAgendamento implements ScheduledAction {


    // Instâncias de JapeWrapper para interagir com as tabelas do Sankhya
    private JapeWrapper cabDAO = JapeFactory.dao("CabecalhoNota");
    private JapeWrapper iteDAO = JapeFactory.dao("ItemNota");

    @Override
    public void onTime(ScheduledActionContext sctx) {

        AuthenticationInfo authenticationInfo = new AuthenticationInfo("SUP",
                BigDecimal.ZERO, BigDecimal.ZERO, 0);
        authenticationInfo.makeCurrent();

        JapeSessionContext.putProperty("usuario_logado", authenticationInfo.getUserID());

        EntityFacade dwf = EntityFacadeFactory.getDWFFacade();
        final JdbcWrapper jdbc = dwf.getJdbcWrapper();
        BigDecimal vnunota = BigDecimal.ZERO;

        try {

            NativeSql ns1 = new NativeSql(jdbc, getClass(), "nfsaidatransferencia.sql");

            ResultSet rs1 = ns1.executeQuery();

            while (rs1.next()) {

                vnunota = rs1.getBigDecimal("NUNOTA");

                BigDecimal nunota = BigDecimal.ZERO;

                nunota = gerarEntradaTransf(rs1);

                if (!BigDecimalUtil.isNullOrZero(nunota)) {

                    DynamicVO vendaVO = JapeFactory.dao("CabecalhoNota")
                            .findOne("NUNOTA=?", new Object[]{vnunota});

                    cabDAO.prepareToUpdateByPK(vendaVO.asBigDecimal("NUNOTA"))
                            .set("NUREM", nunota)
                            .update();

                    recalcularvalor(nunota);
                }

            }

            rs1.close();

        } catch (Exception e1) {

            e1.printStackTrace();

        }

    }

    private void recalcularvalor(BigDecimal nunota) throws Exception {

        ImpostosHelpper impostosHelper = new ImpostosHelpper();
        impostosHelper.setForcarRecalculo(true);
        impostosHelper.carregarNota(nunota);
        impostosHelper.calcularImpostos(nunota);
        impostosHelper.totalizarNota(nunota);
        impostosHelper.salvarNota();

    }

    public BigDecimal gerarEntradaTransf(ResultSet rs1) throws Exception {

        System.out.println("##### Inserindo Cabeçalho da entrada Nr NF " + rs1.getBigDecimal("NUNOTA"));

        DynamicVO newCabecalho = cabDAO.create()
                .set("CODEMP", rs1.getBigDecimal("CODPARC"))
                .set("CODEMPNEGOC", rs1.getBigDecimal("CODPARC"))
                .set("NUMNOTA", rs1.getBigDecimal("NUMNOTA"))
                .set("CODPARC", rs1.getBigDecimal("CODEMP"))
                .set("CODVEND", rs1.getBigDecimal("CODVEND"))
                .set("CODNAT", BigDecimal.valueOf(99001000))
                .set("STATUSNOTA", "A")
                .set("DTNEG", rs1.getTimestamp("DTNEG"))
                .set("NUREM", rs1.getBigDecimal("NUNOTA"))
                .set("DTMOV", rs1.getTimestamp("DTMOV"))
                .set("DTFATUR", rs1.getTimestamp("DTFATUR"))
                .set("CODTIPOPER", rs1.getBigDecimal("TOPENTRADA"))
                .set("CODCENCUS", BigDecimal.valueOf(101007))
                .set("CODTIPVENDA", rs1.getBigDecimal("CODTIPVENDA"))
                .set("SERIENOTA", rs1.getString("SERIENOTA"))
                .set("CHAVENFE", rs1.getString("CHAVENFE"))
                .set("VLRNOTA", rs1.getBigDecimal("VLRNOTA"))
                .set("OBSERVACAO", "Entrada de Transferência gerada automaticamente Pela NF de Transferencia de Nr unico " + rs1.getBigDecimal("NUNOTA"))
                .save();

        BigDecimal nunota = newCabecalho.asBigDecimal("NUNOTA");
        inserirItens(nunota, rs1.getBigDecimal("NUNOTA"));

        return nunota;

    }

    private void inserirItens(BigDecimal nunota, BigDecimal nunotav) throws Exception {

        System.out.println("Inserindo Itens Nr NF " + nunota.toString());

        Collection<DynamicVO> itensVO = JapeFactory.dao("ItemNota")
                .find("NUNOTA=?", new Object[]{nunotav});

        for (DynamicVO iteVO : itensVO) {

            DynamicVO ite = iteDAO.create()
                    .set("NUNOTA", nunota)
                    .set("CODPROD", iteVO.asBigDecimal("CODPROD"))
                    .set("CODLOCALORIG", iteVO.asBigDecimal("CODLOCALORIG"))
                    .set("QTDNEG", iteVO.asBigDecimal("QTDNEG"))
                    .set("ATUALESTOQUE", BigDecimal.ZERO)
                    .set("RESERVA", "N")
                    .set("VLRUNIT", iteVO.asBigDecimal("VLRUNIT"))
                    .set("VLRTOT", iteVO.asBigDecimal("VLRTOT"))
                    .set("BASEIPI", iteVO.asBigDecimal("BASEIPI"))
                    .set("BASEICMS", iteVO.asBigDecimal("BASEICMS"))
                    .set("CODVOL", iteVO.asString("CODVOL"))
                    .save();

            CentralItemNota itemNota = new CentralItemNota();

            itemNota.inicializaNota(nunota);
            itemNota.inicializaNota(nunota,iteVO.asBigDecimal("CODPROD"));


        }
    }


}
