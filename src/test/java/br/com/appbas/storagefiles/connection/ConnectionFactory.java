package br.com.appbas.storagefiles.connection;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.util.ArrayList;
import java.util.Collection;

import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.FixMethodOrder;
import org.junit.Test;
import org.junit.runners.MethodSorters;

@FixMethodOrder(value = MethodSorters.NAME_ASCENDING)
public class ConnectionFactory {

	private static Connection CONNECTION;
	private final Integer IDINSTITUICAO = 1;
	private final LocalDate DATA_MOVIMENTO = LocalDate.parse("2023-02-04");
	private final LocalDateTime DATA_HORA = LocalDateTime.parse("2023-02-04T18:01:13.614454");
	private final String DESCRICAO = "Descricao Teste";

	@BeforeClass
	public static void beforeClass() throws SQLException {
		CONNECTION = DriverManager.getConnection("jdbc:sqlite:sample.db");
	}

	@AfterClass
	public static void afterClass() throws SQLException {
		CONNECTION.close();
	}

	@Test
	public void test1_openConnectionTest() {

		try {

			String create = "CREATE TABLE validacao_consolidacao ( \n" + "   id INTEGER NOT NULL PRIMARY KEY, \n"
					+ "   idInstituicao INTEGER NOT NULL, \n" + "   dataMovimento TEXT NOT NULL ,\n"
					+ "   dataHora TEXT NOT NULL, \n" + "   descricao BLOB NOT NULL \n" + ");";

			CONNECTION.prepareStatement("DROP TABLE IF EXISTS validacao_consolidacao").execute();
			CONNECTION.prepareStatement(create).executeUpdate();

			try (ResultSet rs = CONNECTION.prepareStatement("SELECT COUNT(*) FROM validacao_consolidacao")
					.executeQuery();) {
				Integer total = rs.getInt(1);
				Assert.assertTrue(total == 0);
			}

		} catch (SQLException e) {
			Assert.fail(e.getMessage());
		}

	}

	@Test
	public void test2_insertDadosValidacaoConsolidacaoTest() throws SQLException {
		try (PreparedStatement pstm = CONNECTION.prepareStatement(
				"INSERT INTO validacao_consolidacao ( idInstituicao, dataMovimento, dataHora, descricao) VALUES ( ?, ?, ?, ?)");
				PreparedStatement pstmSelect = CONNECTION.prepareStatement(
						"SELECT COUNT(1) as total FROM validacao_consolidacao WHERE idInstituicao = ?")) {

			pstm.setObject(1, IDINSTITUICAO);
			pstm.setString(2, DATA_MOVIMENTO.toString());
			pstm.setString(3, DATA_HORA.toString());
			pstm.setString(4, DESCRICAO);

			pstm.executeUpdate();

			pstmSelect.setInt(1, 1);

			try (ResultSet rs = pstmSelect.executeQuery();) {
				int total = rs.getInt("total");
				Assert.assertTrue(1 == total);
			}

		}
	}

	@Test
	public void test3_selectDadosValidacaoConsolidacaoTest() throws SQLException {

		try (PreparedStatement pstm = CONNECTION.prepareStatement(
				"SELECT id, idInstituicao, dataMovimento, dataHora, descricao FROM validacao_consolidacao");
				ResultSet rs = pstm.executeQuery();) {
			
			final Collection<ValidacaoConsolidacaoDTO> registros = new ArrayList<>();
			ValidacaoConsolidacaoDTO registro;
			if (rs.next()) {
				registro = new ValidacaoConsolidacaoDTO(
						rs.getInt(1), rs.getInt(2), LocalDate.parse(rs.getString(3)), LocalDateTime.parse(rs.getString(4)), rs.getString(5));
				registros.add(registro);
			}

			while(rs.next()) {
				registro = new ValidacaoConsolidacaoDTO(
						rs.getInt(1), rs.getInt(2), LocalDate.parse(rs.getString(3)), LocalDateTime.parse(rs.getString(4)), rs.getString(5));
				registros.add(registro);
			}

			Assert.assertTrue(5 == rs.getMetaData().getColumnCount());
			Assert.assertTrue(1 == registros.size());
			Assert.assertTrue(registros.stream().filter(validacao -> {
				Assert.assertNotNull(validacao.getId());
				return validacao.getIdInstituicao().equals(IDINSTITUICAO) && validacao.getDataMovimento().equals(DATA_MOVIMENTO) 
						&& validacao.getDataHora().equals(DATA_HORA) && validacao.getDescricao().equals(DESCRICAO);
			}).count() > 0l);
		}
	}
	
	@Test
	public void test4_deleteDadosValidacaoConsolidacaoTest() throws SQLException {
		try (PreparedStatement pstmDelete = CONNECTION.prepareStatement("DELETE FROM validacao_consolidacao");
				PreparedStatement pstmSelect = CONNECTION.prepareStatement(
						"SELECT COUNT(1) as total FROM validacao_consolidacao")) {
			pstmDelete.executeUpdate();
			
			try (ResultSet rs = pstmSelect.executeQuery();) {
				int total = rs.getInt("total");
				Assert.assertTrue(0 == total);
			}
		}
	}

	private class ValidacaoConsolidacaoDTO {
		private Integer id;
		private Integer idInstituicao;
		private LocalDate dataMovimento;
		private LocalDateTime dataHora;
		private String descricao;

		public ValidacaoConsolidacaoDTO(Integer id, Integer idInstituicao, LocalDate dataMovimento,
				LocalDateTime dataHora, String descricao) {
			super();
			this.id = id;
			this.idInstituicao = idInstituicao;
			this.dataMovimento = dataMovimento;
			this.dataHora = dataHora;
			this.descricao = descricao;
		}

		public Integer getId() {
			return id;
		}

		public Integer getIdInstituicao() {
			return idInstituicao;
		}

		public LocalDate getDataMovimento() {
			return dataMovimento;
		}

		public LocalDateTime getDataHora() {
			return dataHora;
		}

		public String getDescricao() {
			return descricao;
		}

	}

}
