package io.sustc.service.impl;

import io.sustc.dto.AuthInfo;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import javax.sql.DataSource;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;

@Service
@Slf4j
public class VerifyAuth {
	@Autowired
	private static DataSource dataSource;

	public static long verifyAuth(AuthInfo auth) {
		// handle invalid auth
		long auth_mid_mid = -1, auto_qq_mid = -1, auth_wx_mid = -1;
		String autoByMidSQL = "select mid from user_info where mid = ? and pwd = ? and active = true";
		String autoByQQSQL = "select mid from user_info where qqid = ? and active = true";
		String autoByWxSQL = "select mid from user_info where wxid = ? and active = true";
		try (Connection conn = dataSource.getConnection();
		     PreparedStatement stmt = conn.prepareStatement(autoByMidSQL)) {
			stmt.setLong(1, auth.getMid());
			stmt.setString(2, EncryptSHA256.encrypt(auth.getPassword()));
			try (ResultSet rs = stmt.executeQuery()) {
				if (rs.next()) {
					auth_mid_mid = rs.getLong("mid");
				}
			}
		} catch (SQLException e) {
			log.error("SQL error: {}", e.getMessage());
			return -1;
		}
		try (Connection conn = dataSource.getConnection();
		     PreparedStatement stmt = conn.prepareStatement(autoByQQSQL)) {
			stmt.setString(1, auth.getQq());
			try (ResultSet rs = stmt.executeQuery()) {
				if (rs.next()) {
					auto_qq_mid = rs.getLong("mid");
				}
			}
		} catch (SQLException e) {
			log.error("SQL error: {}", e.getMessage());
			return -1;
		}
		try (Connection conn = dataSource.getConnection();
		     PreparedStatement stmt = conn.prepareStatement(autoByWxSQL)) {
			stmt.setString(1, auth.getWechat());
			try (ResultSet rs = stmt.executeQuery()) {
				if (rs.next()) {
					auth_wx_mid = rs.getLong("mid");
				}
			}
		} catch (SQLException e) {
			log.error("SQL error: {}", e.getMessage());
			return -1;
		}
		if (auth_mid_mid == -1 && auto_qq_mid == -1 && auth_wx_mid == -1) {
			log.error("Invalid auth.");
			return -1;
		}
		if (auto_qq_mid != -1 && auth_wx_mid != -1 && auto_qq_mid != auth_wx_mid) {
			log.error("OIDC via QQ and WeChat contradicts.");
			return -1;
		}
		return auth_mid_mid != -1 ? auth_mid_mid : auto_qq_mid != -1 ? auto_qq_mid : auth_wx_mid;
	}
}
