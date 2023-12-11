package io.sustc.service.impl;

import io.sustc.dto.PostVideoReq;
import lombok.Data;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import javax.sql.DataSource;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.time.LocalDateTime;

@Service
@Slf4j
public class VerifyVideoReq {
	@Autowired
	private static DataSource dataSource;

	public static Boolean verifyVideoReq(PostVideoReq req, long auth_mid) {
		// handle invalid title
		if (req.getTitle() == null || req.getTitle().isEmpty()) {
			log.error("Video title is empty.");
			return false;
		}
		// handle invalid duration
		if (req.getDuration() < 10) {
			log.error("The video is too short (less than 10 seconds).");
			return false;
		}
		// handle invalid public time
		if (req.getPublicTime() != null && req.getPublicTime().toLocalDateTime().isBefore(LocalDateTime.now())) {
			log.error("Public time is earlier than current time.");
			return false;
		}
		// handle duplicate video
		String dupCheckSQL = "select count(*) from video_info where title = ? and ownMid = ? and active = true";
		try (Connection conn = dataSource.getConnection();
		     PreparedStatement stmt = conn.prepareStatement(dupCheckSQL)) {
			stmt.setString(1, req.getTitle());
			stmt.setLong(2, auth_mid);
			try (ResultSet rs = stmt.executeQuery()) {
				rs.next();
				if (rs.getInt(1) > 0) {
					log.error("Title has been used.");
					return false;
				}
			}
		} catch (SQLException e) {
			log.error("SQL error: {}", e.getMessage());
			return false;
		}
		return true;
	}
}
