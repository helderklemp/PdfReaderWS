/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package br.intercomex.pdfreader;

import java.io.IOException;
import java.sql.Blob;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.logging.Level;
import java.util.logging.Logger;
import javax.jws.WebService;
import javax.jws.WebMethod;
import javax.jws.WebParam;
import javax.naming.Context;
import javax.naming.InitialContext;
import javax.naming.NamingException;
import javax.sql.DataSource;
import org.apache.pdfbox.pdmodel.PDDocument;
import org.apache.pdfbox.util.PDFTextStripper;

/**
 *
 * @author helderklemp
 */
@WebService(serviceName = "PdrFeaderWS")
public class PdrFeaderWS {

    private Connection conn = null;

    @WebMethod(operationName = "getPdfContent")
    public String getPdfContent(@WebParam(name = "sql") String sql) {
        String text = null;
        Statement stmt = null;
        try {
            getConnection();
            stmt = conn.createStatement();
            ResultSet rs = stmt.executeQuery(sql);
            Blob pdfParam = null;
            while (rs.next()) {
                pdfParam = rs.getBlob(1);
            }
            text = getPdf(pdfParam);
            stmt.close();
            rs.close();
            
        } catch (SQLException ex) {
            Logger.getLogger(PdrFeaderWS.class.getName()).log(Level.SEVERE, null, ex);
        } catch (IOException ex) {
            Logger.getLogger(PdrFeaderWS.class.getName()).log(Level.SEVERE, null, ex);
        } catch (NamingException ex) {
            Logger.getLogger(PdrFeaderWS.class.getName()).log(Level.SEVERE, null, ex);
        }

        return text;
    }

    public static void main(String args[]) {
        PdrFeaderWS ws = new PdrFeaderWS();
        ws.processamentoPDFLote(null);
    }

    /**
     * Metodo responsavel por pegar todos os registros da tabela RE_SUFIXO ,
     * para cada uma extrarir os dados do PDF E adicionar nas colunas de data e
     * numero
     */
    @WebMethod(operationName = "processamentoPDFLote")
    public void processamentoPDFLote(@WebParam(name = "anoProcessamento") String anoProcessamento) {

        Statement stmt = null;
        ResultSet rs ;
        PreparedStatement pstmSelect=null;
        try {
            getLocalConnection();
            String sql = "select arq_ce , NR_PROCESSO , ANO_PROCESSO from processo where MIMETYPE_CE = 'application/pdf' and NR_CE is null and DT_CE is null ";
            if(anoProcessamento!=null&&!anoProcessamento.equals(""))
            {
                sql="select arq_ce , NR_PROCESSO , ANO_PROCESSO from processo where MIMETYPE_CE = 'application/pdf' and NR_CE is null and DT_CE is null and ANO_PROCESSO=?";
                pstmSelect = conn.prepareStatement(sql);
                pstmSelect.setString(1, anoProcessamento);
                rs = pstmSelect.executeQuery();
            }else
            {
                stmt = conn.createStatement();
                rs = stmt.executeQuery(sql);
            }
            
            Blob pdfParam = null;
            while (rs.next()) {
                pdfParam = rs.getBlob(1);
                int nrProcesso = rs.getInt(2);
                int anoProcesso = rs.getInt(3);
                String text = null;
                text = getPdf(pdfParam);
                
                System.out.println("NR_PROCESSO:===" + nrProcesso);
                Date dtDocumento = getDataDocumento(text);
                String nrDocumento = getNumeroDocumento(text);
                //System.out.println(text);  
                String sqlUpdate = "UPDATE processo set NR_CE=? , DT_CE=? where NR_PROCESSO=? and ANO_PROCESSO=?";
                PreparedStatement pstm = conn.prepareStatement(sqlUpdate);
                if (dtDocumento != null && nrDocumento != null) {
                  
                    pstm.setString(1, nrDocumento);
                    pstm.setDate(2, new java.sql.Date(dtDocumento.getTime()));
                    pstm.setInt(3, nrProcesso);
                    pstm.setInt(4, anoProcesso);
                    pstm.execute();
                    pstm.close();
                }

            }
            if(anoProcessamento!=null&&!anoProcessamento.equals(""))
            {
                pstmSelect.close();
            }else
            {
                stmt.close();
            }
            
            rs.close();
            

        } catch (NamingException ex) {
            Logger.getLogger(PdrFeaderWS.class.getName()).log(Level.SEVERE, null, ex);
        } catch (SQLException ex) {
            Logger.getLogger(PdrFeaderWS.class.getName()).log(Level.SEVERE, null, ex);
        } catch (IOException ex) {
            Logger.getLogger(PdrFeaderWS.class.getName()).log(Level.SEVERE, null, ex);
        } catch (ClassNotFoundException ex) {
            Logger.getLogger(PdrFeaderWS.class.getName()).log(Level.SEVERE, null, ex);
        }
    }

    private Connection getConnection() throws NamingException, SQLException {
        if (conn == null) {
            Context initContext = new InitialContext();
            Context envContext = (Context) initContext.lookup("java:/comp/env");
            DataSource ds = (DataSource) envContext.lookup("jdbc/IntercomexDS");
            conn = ds.getConnection();
        }
        return conn;
    }

    private Connection getLocalConnection() throws NamingException, SQLException, ClassNotFoundException {
        if (conn == null) {
            Class.forName("oracle.jdbc.driver.OracleDriver");
            String oracleURL = "jdbc:oracle:thin:@intercomex.maxapex.net:1521:XE";
            conn = DriverManager.getConnection(oracleURL, "appcomex", "xoxotaxo");
        }
        return conn;
    }

    private String getPdf(Blob pdf) throws SQLException, IOException {
        String text = null;
        PDDocument document = PDDocument.load(pdf.getBinaryStream());
        PDFTextStripper stripper = new PDFTextStripper("UTF-16");
        text = stripper.getText(document);
        document.close();
        return text;
    }

    private Date getDataDocumento(String pdf) {
        Date data = null;
        if (pdf != null) {
            int i = pdf.indexOf("DATA DE EMISSAO DO COMPROVANTE:");
            String dtStr = null;
            int tamanhoStr = pdf.length();
            if (tamanhoStr > 10) {
                int posicaoInicial = i + 32;
                int posicaoFinal = i + 43;
                if (tamanhoStr < posicaoFinal) {
                    System.out.println("PROBLEMA DE TAMANHO PDF=====" + tamanhoStr);
                    posicaoFinal = tamanhoStr;
                }
                dtStr = pdf.substring(posicaoInicial, posicaoFinal);
                dtStr = dtStr.trim();
                SimpleDateFormat sdf = new SimpleDateFormat("dd/MM/yyyy");
                System.out.println("Data=====" + dtStr);
                try {
                    data = sdf.parse(dtStr);
                } catch (ParseException ex) {
                    Logger.getLogger(PdrFeaderWS.class.getName()).log(Level.SEVERE, null, ex);
                }
            }

        }
        return data;
    }

    private String getNumeroDocumento(String pdf) {
        String numero = null;
        int posicaoInicial=34;
        int posicaoFinal=50;
        if (pdf != null) {
            int i = pdf.indexOf("COMPROVANTE DE EXPORTACAO NUMERO");
            if(i== -1){
                i = pdf.indexOf("COMPROVANTE DE EXPORTACAO-DSE - NUMERO");
                posicaoInicial=39;
                posicaoFinal = 55;
                
            }
            int tamanhoStr = pdf.length();
            if (tamanhoStr > 10) {
                numero = pdf.substring(i + posicaoInicial, i + posicaoFinal);
                numero = numero.trim();
                numero = numero.replaceAll("/", "");
                System.out.println("Numero=====" + numero);
                try
                {
                    Long.parseLong(numero);
                }catch(NumberFormatException ex)
                {
                    numero=null; 
                    Logger.getLogger(PdrFeaderWS.class.getName()).log(Level.SEVERE, null, ex);
                }
            }

        }
        
        return numero;
    }
}
